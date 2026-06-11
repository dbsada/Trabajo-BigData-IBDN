"""
Pipeline de despliegue: orquesta la compilación del JAR, arranque de servicios,
carga de datos inicial, y lanzamiento del job de predicción.

Uso:
    python3 start.py                # Despliegue local con Docker
    python3 start.py --build        # Despliegue local incluyendo la fase de build
    python3 start.py --stop         # Detener y limpiar contenedores y volúmenes
"""

import os
import subprocess
import sys
import time

# ─── Leer variables de entorno (desde .env) ─────────────────────
_KAFKA = os.getenv("KAFKA", "kafka:9092")
_TOPIC_IN = os.getenv("TOPIC_IN", "request")
_TOPIC_OUT = os.getenv("TOPIC_OUT", "response")
_MINIO_USER = os.getenv("MINIO_USER", "admin")
_MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "password")
_MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
_MLFLOW_URI = os.getenv("MLFLOW_URI", "http://mlflow:5000")

def log(msg):
    print(f"\n▸ {msg}")


def run(cmd, **kwargs):
    kwargs.setdefault("timeout", 120)
    kwargs.setdefault("capture_output", True)
    kwargs.setdefault("shell", True)
    quiet = kwargs.pop("quiet", False)
    if not quiet:
        print(f"  $ {cmd[:130]}...")
    r = subprocess.run(cmd, **kwargs)
    if r.returncode != 0 and not quiet:
        err = r.stderr.decode().strip()[:200] if r.stderr else "?"
        print(f"  ⚠️  {err}")
    return r


def wait_for_container(container_name, cmd_check, timeout_min=5):
    """Espera a que un contenedor exista y luego a que responda al check."""
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        # Primero esperar a que el contenedor exista
        r = run(f"docker ps -q --filter name={container_name}", quiet=True)
        if not r.stdout.decode().strip():
            time.sleep(5)
            continue
        # Luego esperar a que responda al comando de verificación
        if run(cmd_check, quiet=True).returncode == 0:
            print(f"  ✅ {container_name}")
            return
        time.sleep(5)
    print(f"  ⚠️  Timeout: {container_name}")

# ═══════════════════════════════════════════════════════════════
# 1. Compilar JAR
# ═══════════════════════════════════════════════════════════════

def compile_jar():
    log("Compilando JAR de Spark...")
    if not os.path.exists("spark-jobs/target/scala-2.13/spark-jobs_2.13-0.1.jar"):
        run("cd spark-jobs && sbt package", timeout=180)


# ═══════════════════════════════════════════════════════════════
# 2. Pipeline de datos
# ═══════════════════════════════════════════════════════════════

def data_pipeline(kexec_prefix, is_docker=True):
    """Crea buckets, descarga datos, sube a MinIO, crea topics, importa distancias."""
    mc = "docker exec minio" if is_docker else "kubectl exec -n ibdn deploy/minio"
    kc = "docker exec kafka" if is_docker else "kubectl exec -n ibdn deploy/kafka"

    log("Pipeline de datos")

    run(f'{kexec_prefix} sh -c "MINIO_LOCAL_ENDPOINT={_MINIO_ENDPOINT} '
        f'python3 /app/scripts/create_bucket.py"', timeout=300)
    run(f'{kexec_prefix} sh -c "MINIO_LOCAL_ENDPOINT={_MINIO_ENDPOINT} '
        f'python3 /app/scripts/download_data.py"', timeout=300)

    run(f'{mc} sh -c "mkdir -p /tmp/data && mc alias set local http://localhost:9000 {_MINIO_USER} {_MINIO_PASSWORD}" 2>/dev/null',
        quiet=True)
    for fname in ["simple_flight_delay_features.jsonl.bz2", "origin_dest_distances.jsonl"]:
        run(f'docker cp -q data/{fname} minio:/tmp/data/{fname}', timeout=60)
        run(f'{mc} mc cp /tmp/data/{fname} local/lakehouse/raw/', quiet=True)

    for topic in [_TOPIC_IN, _TOPIC_OUT]:
        run(f'{kc} /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 '
            f'--topic {topic} --partitions 1 --replication-factor 1 --if-not-exists', quiet=True)
    log("Topics Kafka creados")

    for _ in range(10):
        r = run(f'{kexec_prefix} python3 /app/scripts/init_cassandra.py', timeout=120, quiet=True)
        if r.returncode == 0:
            print(f"    Distancias: {r.stdout.decode().strip()}")
            break
        print("    ⏳ Cassandra no lista, reintentando...")
        time.sleep(10)
    log("Cassandra inicializada con distancias")


# ═══════════════════════════════════════════════════════════════
# 3. Lanzar MakePrediction
# ═══════════════════════════════════════════════════════════════

def start_prediction():
    """Lanza el job de streaming MakePrediction en Spark (se queda ejecutándose)."""
    log("Iniciando MakePrediction...")

    jar_path = "/app/spark-jobs/target/scala-2.13/spark-jobs_2.13-0.1.jar"
    spark_conf = (
        '--conf spark.cores.max=2 '
        '--conf spark.executor.memory=2g '
        f'--conf spark.hadoop.fs.s3a.endpoint={_MINIO_ENDPOINT} '
        f'--conf spark.hadoop.fs.s3a.access.key={_MINIO_USER} '
        f'--conf spark.hadoop.fs.s3a.secret.key={_MINIO_PASSWORD} '
        f'--conf spark.hadoop.fs.s3a.list.version=1 '
        f'--conf spark.driverEnv.MLFLOW_TRACKING_URI={_MLFLOW_URI} '
        f'--conf spark.driverEnv.MODEL_VERSION=1.0 '
        f'--conf spark.driverEnv.BUCKETIZER_VERSION=1.0'
    )

    run(f'docker exec spark-manager sh -c "spark-submit --master spark://spark-manager:7077 '
        f'--deploy-mode cluster {spark_conf} --class MakePrediction {jar_path}"', timeout=30)
    
    log("MakePrediction lanzado")

# ═══════════════════════════════════════════════════════════════
# DOCKER
# ═══════════════════════════════════════════════════════════════

def deploy_docker(build=True):
    log("=== Despliegue Docker ===")
    compile_jar()
    # Bajar contenedores previos (project name explícito para evitar conflictos)
    run("docker-compose -p flight-delay down --remove-orphans", timeout=120, quiet=True)
    # También limpiar contenedores huérfanos de project names anteriores
    run("docker rm -f $(docker ps -aq --filter name=bd-a-mano) 2>/dev/null || true", timeout=30, quiet=True)

    if build:
        # Construir spark-base primero (imagen pesada compartida por manager y worker)
        log("Construyendo imagen base Spark (spark-base:4.1.1)...")
        r = subprocess.run("docker build -t spark-base:4.1.1 -f docker/dockerfile.spark-base .", shell=True, timeout=10000)
        if r.returncode != 0:
            log("❌ spark-base build falló.")
            sys.exit(1)
        # Construir imágenes individualmente (más rápido que docker-compose build en macOS)
        log("Construyendo imágenes restantes...")
        for service, df in [("flask", "docker/dockerfile.python"),
                            ("spark-worker", "docker/dockerfile.spark"),
                            ("spark-manager", "docker/dockerfile.spark"),
                            ("airflow-webserver", "docker/dockerfile.airflow"),
                            ("airflow-scheduler", "docker/dockerfile.airflow"),
                            ("airflow-init", "docker/dockerfile.airflow"),
                            ("airflow-dag-processor", "docker/dockerfile.airflow")]:
            tag = f"flight-delay-{service}:latest"
            timeout = 1800 if service == "flask" else 900
            r = subprocess.run(f"docker build -t {tag} -f {df} .", shell=True, timeout=timeout)
            if r.returncode != 0:
                log(f"❌ Build de {service} falló.")
                sys.exit(1)
    else:
        log("Omitiendo builds...")

    # Arrancar contenedores (sin timeout — compose retorna cuando los contenedores están creados)
    subprocess.run("docker-compose up -d", shell=True, timeout=600)

    # Esperar servicios (ordenados de más lento a más rápido)
    wait_for_container("spark-manager", "curl -s -o /dev/null http://localhost:8080/ 2>/dev/null")
    wait_for_container("kafka", "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null", timeout_min=10)
    wait_for_container("cassandra", 'docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" 2>/dev/null', timeout_min=10)
    wait_for_container("minio", "curl -s -o /dev/null http://localhost:9000/minio/health/live 2>/dev/null")
    wait_for_container("flask", "curl -s -o /dev/null http://localhost:5001/ 2>/dev/null", timeout_min=3)

    data_pipeline("docker exec flask", is_docker=True)
    start_prediction()
    log("=== Despliegue Docker completado ===")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if "--stop" in sys.argv:
        log("Deteniendo contenedores y eliminando volúmenes...")
        run("docker-compose -p flight-delay down --remove-orphans --volumes", timeout=120, quiet=True)
        log("✅ Todo detenido y volúmenes eliminados")
        sys.exit(0)

    build = "--build" in sys.argv
    print('Iniciando despliegue...')
    deploy_docker(build)