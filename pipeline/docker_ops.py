import os
import logging
import subprocess
import time
from utils.shell import sh
from utils.network import check_port
from config import DeployConfig

@sh
def compose_up(project_home, db):
    return f"cd \"{project_home}\" && docker compose --profile db_{db} up -d"

@sh
def compose_down(project_home):
    return f"cd \"{project_home}\" && docker compose --profile db_mongo --profile db_cassandra down --remove-orphans"

@sh
def compose_logs(service_name, log_file):
    return f"cd {os.path.dirname(os.path.dirname(__file__))} && docker compose logs --no-color {service_name} > {log_file}"

@sh
def less_file(log_file):
    return f"less -S +G {log_file}"

@sh(timeout=5)
def lsof_port(port):
    return f"lsof -i :{port} -sTCP:LISTEN"

def check_ports_busy(cfg):
    ports_to_check = [
        ("Spark UI",      int(os.getenv('SPARK_MASTER_UI_PORT', '8080'))),
        ("Spark Master",  int(os.getenv('SPARK_MASTER_PORT', '7077'))),
        ("Flask",         int(os.getenv('FLASK_PORT', '5001'))),
        ("MinIO API",     int(os.getenv('MINIO_API_PORT', '9000'))),
        ("MinIO Console", int(os.getenv('MINIO_CONSOLE_PORT', '9001'))),
        ("Kafka",         int(os.getenv('KAFKA_PORT', '9092'))),
        ("MLflow",        int(os.getenv('MLFLOW_PORT', '5003'))),
        ("Airflow",       int(os.getenv('AIRFLOW_UI_PORT', '8085'))),
        ("Postgres",      int(os.getenv('AIRFLOW_POSTGRES_PORT', '5432'))),
    ]
    if cfg.db_mode == 'mongo':
        ports_to_check.append(("MongoDB", int(os.getenv('MONGODB_PORT', '27017'))))
    else:
        ports_to_check.append(("Cassandra", int(os.getenv('CASSANDRA_PORT', '9042'))))
    return [(name, port) for name, port in ports_to_check if check_port(port)]

def show_service_logs(service_name):
    log_file = f"logs/docker_{service_name}.log"
    logging.info(f"Updating log file: {log_file}")
    compose_logs(service_name, log_file)
    less_file(log_file)

def upload_to_minio(local_path, minio_key):
    if not os.path.exists(local_path):
        logging.error(f"File not found: {local_path}")
        return None
    access_key = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")
    subprocess.run(
        ["docker", "exec", "minio", "mc", "alias", "set", "local",
         "http://localhost:9000", access_key, secret_key],
        capture_output=True, text=True)
    with open(local_path, 'rb') as f:
        r = subprocess.run(
            ["docker", "exec", "-i", "minio", "mc", "pipe", f"local/{minio_key}"],
            stdin=f, capture_output=True, text=True)
    if r.returncode == 0:
        logging.info(f"Uploaded to MinIO: {minio_key}")
    else:
        logging.error(f"Error uploading {minio_key}: {r.stderr.strip()}")
    return r

def upload_data_to_minio(cfg):
    files = [
        (f"{cfg.project_home}/data/simple_flight_delay_features.jsonl.bz2", "lakehouse/raw"),
        (f"{cfg.project_home}/data/origin_dest_distances.jsonl", "lakehouse/raw"),
        (f"{cfg.project_home}/models/sklearn_vectorizer.pkl", "lakehouse/models"),
        (f"{cfg.project_home}/models/sklearn_regressor.pkl", "lakehouse/models"),
    ]
    for local_path, minio_prefix in files:
        if os.path.exists(local_path):
            key = f"{minio_prefix}/{os.path.basename(local_path)}"
            upload_to_minio(local_path, key)
    logging.info("Files uploaded to MinIO")

@sh
def cassandra_nodetool():
    container = os.getenv('CASSANDRA_CONTAINER', 'cassandra')
    return f"docker exec {container} nodetool status 2>&1 | grep -q '^UN'"

@sh
def cassandra_cqlsh():
    container = os.getenv('CASSANDRA_CONTAINER', 'cassandra')
    return f"docker exec {container} cqlsh -e 'DESCRIBE KEYSPACES' 2>/dev/null"


def is_cassandra_healthy():
    import docker as dkr
    try:
        client = dkr.from_env()
        c = client.containers.get('cassandra')
        if c.status != 'running':
            return False
        if cassandra_nodetool().returncode != 0:
            return False
        return True
    except Exception:
        return False


@sh
def restart_cassandra(project_home):
    return f"cd \"{project_home}\" && docker compose --profile db_cassandra up -d cassandra"

def start_services(cfg):
    from rich.live import Live
    from rich.align import Align
    from rich.table import Table
    from rich.text import Text

    logging.info('Starting services with Docker Compose...')
    os.environ['DB_MODE'] = cfg.db_mode

    busy = check_ports_busy(cfg)
    if busy:
        from rich.panel import Panel
        msg = "[bold red]Ports occupied detected:[/bold red]\n\n"
        pids = set()
        has_docker = False
        for name, port in busy:
            msg += f"  {name} (port {port})"
            lsof = lsof_port(port)
            lines = lsof.stdout.strip().splitlines()
            if len(lines) > 1:
                for line in lines[1:]:
                    parts = line.split()
                    if len(parts) >= 3:
                        is_docker = "docker" in parts[0].lower()
                        if is_docker:
                            has_docker = True
                            msg += f"\n    {parts[0]} (PID {parts[1]}) {parts[2]} [dim](Docker Desktop)[/dim]"
                        else:
                            msg += f"\n    {parts[0]} (PID {parts[1]}) {parts[2]}"
                            pids.add(parts[1])
            msg += "\n"
        if pids:
            msg += f"\n[bold]To free them:[/bold]  kill -9 {' '.join(sorted(pids))}\n"
        if has_docker:
            msg += (
                "\n[bold]Note:[/bold] Docker Desktop has some ports occupied. You can:\n"
                "  Change the conflicting ports in .env\n"
                "  Close Docker Desktop from the system tray if you don't need it\n"
            )
        msg += "\nFree the ports or change the configuration in .env before continuing."
        raise RuntimeError(msg)

    compose_up(cfg.project_home, cfg.db_mode)

    db_label = "MongoDB" if cfg.db_mode == 'mongo' else "Cassandra"
    db_port = int(os.getenv('MONGODB_PORT', '27017')) if cfg.db_mode == 'mongo' else int(os.getenv('CASSANDRA_PORT', '9042'))

    services = [
        {"name": "Kafka",   "port": int(os.getenv('KAFKA_PORT', '9092')),         "ready": False},
        {"name": db_label,  "port": db_port,                                        "ready": False},
        {"name": "Spark",   "port": int(os.getenv('SPARK_MASTER_UI_PORT', '8080')),"ready": False},
        {"name": "Flask",   "port": int(os.getenv('FLASK_PORT', '5001')),           "ready": False},
        {"name": "MinIO",   "port": int(os.getenv('MINIO_API_PORT', '9000')),       "ready": False},
        {"name": "Airflow", "port": int(os.getenv('AIRFLOW_UI_PORT', '8085')),      "ready": False},
    ]

    cassandra_nodetool_pending = False
    cassandra_cql_pending = False
    spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    frame = 0

    def build_table():
        try:
            result = _get_vm_ip()
            vm_ip = result.stdout.strip().split()[0] if result.returncode == 0 else 'localhost'
        except Exception:
            vm_ip = 'localhost'
        table = Table(title="Starting Services")
        table.add_column("Service", style="cyan", min_width=12)
        table.add_column("Status", justify="center", min_width=6, max_width=8)
        table.add_column("Port", style="dim", justify="center", min_width=6, max_width=8)
        table.add_column("URL", style="dim", justify="center", min_width=20)
        for s in services:
            status = s.get("status", "·")
            name_style = "green" if s["ready"] else "grey50"
            url = ""
            if s["name"] in ("Flask", "MinIO", "Spark", "Airflow"):
                url = f"http://{vm_ip}:{s['port']}"
            table.add_row(Text(s["name"], style=name_style), status,
                          str(s["port"]) if s.get("port") else "-", url)
        return table

    with Live(Align.center(build_table()), refresh_per_second=10, screen=False) as live:
        while not all(s["ready"] for s in services):
            frame += 1
            for s in services:
                if s["ready"]:
                    s["status"] = "✓"
                elif check_port(s["port"]):
                    if s["name"] == "Cassandra":
                        if not cassandra_nodetool_pending and not cassandra_cql_pending:
                            cassandra_nodetool_pending = True
                        s["status"] = spinner[frame % len(spinner)]
                    else:
                        s["ready"] = True
                        s["status"] = "✓"
                else:
                    s["status"] = spinner[frame % len(spinner)]

            if cassandra_nodetool_pending:
                if cassandra_nodetool().returncode == 0:
                    cassandra_nodetool_pending = False
                    cassandra_cql_pending = True

            if cassandra_cql_pending:
                if cassandra_cqlsh().returncode == 0:
                    for s in services:
                        if s["name"] == "Cassandra":
                            s["ready"] = True
                            s["status"] = "✓"
                    cassandra_cql_pending = False

            live.update(Align.center(build_table()))
            time.sleep(0.08)

    logging.info('All services are ready.')


def start_services_silent(cfg):
    logging.info('Starting services with Docker Compose...')
    os.environ['DB_MODE'] = cfg.db_mode

    busy = check_ports_busy(cfg)
    if busy:
        names = [f"{n} ({p})" for n, p in busy]
        raise RuntimeError(f"Ports occupied: {', '.join(names)}. Free them or change .env before continuing.")

    compose_up(cfg.project_home, cfg.db_mode)

    db_port = int(os.getenv('MONGODB_PORT', '27017')) if cfg.db_mode == 'mongo' else int(os.getenv('CASSANDRA_PORT', '9042'))

    services = [
        {"name": "Kafka",   "port": int(os.getenv('KAFKA_PORT', '9092')),         "ready": False},
        {"name": "DB",      "port": db_port,                                        "ready": False},
        {"name": "Spark",   "port": int(os.getenv('SPARK_MASTER_UI_PORT', '8080')),"ready": False},
        {"name": "Flask",   "port": int(os.getenv('FLASK_PORT', '5001')),           "ready": False},
        {"name": "MinIO",   "port": int(os.getenv('MINIO_API_PORT', '9000')),       "ready": False},
        {"name": "Airflow", "port": int(os.getenv('AIRFLOW_UI_PORT', '8085')),      "ready": False},
    ]

    cassandra_nodetool_pending = False
    cassandra_cql_pending = False

    while not all(s["ready"] for s in services):
        for s in services:
            if s["ready"]:
                continue
            if check_port(s["port"]):
                if s["name"] == "DB" and cfg.db_mode == 'cassandra':
                    if not cassandra_nodetool_pending and not cassandra_cql_pending:
                        cassandra_nodetool_pending = True
                else:
                    s["ready"] = True
            else:
                pass

        if cassandra_nodetool_pending:
            if cassandra_nodetool().returncode == 0:
                cassandra_nodetool_pending = False
                cassandra_cql_pending = True

        if cassandra_cql_pending:
            if cassandra_cqlsh().returncode == 0:
                for s in services:
                    if s["name"] == "DB":
                        s["ready"] = True
                cassandra_cql_pending = False

        time.sleep(0.5)

    logging.info('All services are ready.')

@sh
def _get_vm_ip():
    return 'hostname -I'


def wait_for_flask(cfg):
    from utils.network import wait_for_http
    vm_ip = 'localhost'
    try:
        result = _get_vm_ip()
        vm_ip = result.stdout.strip().split()[0]
    except Exception:
        pass
    flk_port = os.getenv('FLASK_PORT', '5001')
    if not wait_for_http(f"http://{vm_ip}:{flk_port}/", timeout=60):
        raise RuntimeError(f"Flask did not start within 60s at http://{vm_ip}:{flk_port}")


def wait_for_infra(cfg):
    from utils.network import wait_for_http
    vm_ip = 'localhost'
    try:
        result = _get_vm_ip()
        vm_ip = result.stdout.strip().split()[0]
    except Exception:
        pass
    spark_port = int(os.getenv('SPARK_MASTER_UI_PORT', '8080'))
    minio_port = int(os.getenv('MINIO_API_PORT', '9000'))
    mlflow_port = int(os.getenv('MLFLOW_PORT', '5003'))
    airflow_port = int(os.getenv('AIRFLOW_UI_PORT', '8085'))
    if not wait_for_http(f"http://{vm_ip}:{spark_port}/", timeout=120):
        raise RuntimeError(f"Spark did not start within 120s")
    if not wait_for_http(f"http://{vm_ip}:{minio_port}/", timeout=60):
        raise RuntimeError(f"MinIO did not start within 60s")
    if not wait_for_http(f"http://{vm_ip}:{mlflow_port}/", timeout=90):
        raise RuntimeError(f"MLflow did not start within 60s")
    if not wait_for_http(f"http://{vm_ip}:{airflow_port}/", timeout=120):
        raise RuntimeError(f"Airflow did not start within 120s")