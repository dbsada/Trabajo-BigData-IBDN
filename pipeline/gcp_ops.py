import os
import time
import subprocess
import logging
import tempfile
from utils.shell import sh
from config import DeployConfig

CLOUD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloud")

class GCPConfig:
    def __init__(self):
        self.project = os.getenv("GCP_PROJECT") or self._detect_project()
        self.zone = os.getenv("GCP_ZONE", "europe-west1-b")
        self.instance = os.getenv("GCP_INSTANCE", "bigdata-vm")
        self.user = os.getenv("GCP_USER", "ubuntu")
        self.repo = os.getenv("REMOTE_REPO", "~/ibdn")

    def _detect_project(self):
        try:
            r = subprocess.run(
                ["gcloud", "config", "get-value", "project"],
                capture_output=True, text=True, check=True)
            return r.stdout.strip()
        except Exception:
            raise RuntimeError("GCP_PROJECT not set and could not detect from gcloud config. Set it in .env")

def _gcloud_cmd(gcp, *args):
    return f"gcloud compute --project {gcp.project} {' '.join(args)}"

def _ssh_cmd(gcp, command):
    escaped = command.replace("'", "'\\''")
    return (
        f'gcloud compute ssh {gcp.user}@{gcp.instance} '
        f"--zone {gcp.zone} --command '{escaped}' --quiet "
        f"--project {gcp.project}"
    )

def _check_or_fail(r, label=""):
    if r.returncode != 0:
        logging.error(f"{label} failed (exit {r.returncode})\n{r.stderr.strip()}")
        raise RuntimeError(f"{label} (exit {r.returncode})")
    return r

# ---- VM Control ----
@sh
def _gcloud_describe_instance(gcp):
    return _gcloud_cmd(gcp, "instances", "describe", gcp.instance, "--zone", gcp.zone)

def vm_exists(gcp):
    return _gcloud_describe_instance(gcp).returncode == 0

@sh
def _gcloud_create_vm(gcp):
    cloud_init = os.path.join(CLOUD_DIR, "cloud-init.yaml")
    return (
        f"gcloud compute instances create {gcp.instance} "
        f"--zone {gcp.zone} --machine-type=e2-standard-4 "
        f"--image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud "
        f"--boot-disk-size=50 "
        f"--metadata-from-file user-data={cloud_init} "
        f"--project {gcp.project}"
    )

def create_vm(gcp):
    logging.info("Creating VM...")
    r = _gcloud_create_vm(gcp)
    _check_or_fail(r, "VM creation")
    logging.info("VM created.")

@sh(check=True)
def start_vm(gcp):
    return _gcloud_cmd(gcp, "instances", "start", gcp.instance, "--zone", gcp.zone)

@sh(check=True)
def stop_vm(gcp):
    return _gcloud_cmd(gcp, "instances", "stop", gcp.instance, "--zone", gcp.zone)

@sh
def _ssh_echo_ready(gcp):
    return _ssh_cmd(gcp, "echo ready")

@sh
def _ssh_docker_compose_version(gcp):
    return _ssh_cmd(gcp, "docker compose version")

def wait_for_vm(gcp, timeout=180):
    for _ in range(timeout):
        if _ssh_echo_ready(gcp).returncode == 0:
            for _ in range(timeout // 2):
                if _ssh_docker_compose_version(gcp).returncode == 0:
                    return
                time.sleep(2)
            raise TimeoutError("Docker compose not ready after provisioning")
        time.sleep(2)
    raise TimeoutError(f"VM not ready after {timeout}s")

@sh
def _gcloud_get_external_ip(gcp):
    return (
        f"gcloud compute instances describe {gcp.instance} "
        f"--zone {gcp.zone} "
        f"--format=value(networkInterfaces[0].accessConfigs[0].natIP) "
        f"--project {gcp.project}"
    )

def get_external_ip(gcp):
    r = _gcloud_get_external_ip(gcp)
    return r.stdout.strip() if r.returncode == 0 else "N/A"

# ---- Deploy ----
@sh
def _ssh_check_repo(gcp):
    return _ssh_cmd(gcp, f"test -d {gcp.repo}/.git")

@sh
def _ssh_git_pull(gcp):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && git fetch origin main && git reset --hard origin/main")

@sh
def _ssh_git_clone(gcp):
    github_repo = os.getenv("GITHUB_REPO")
    if not github_repo:
        raise RuntimeError("GITHUB_REPO not set in .env")
    return _ssh_cmd(gcp, f"git clone {github_repo} {gcp.repo}")

def deploy_code(gcp):
    r = _ssh_check_repo(gcp)
    if r.returncode == 0:
        logging.info("Pulling latest code...")
        _check_or_fail(_ssh_git_pull(gcp), "git pull")
    else:
        logging.info("Cloning repository...")
        _check_or_fail(_ssh_git_clone(gcp), "git clone")

@sh
def _ssh_deploy_env(gcp):
    registry = os.getenv('ARTIFACT_REGISTRY', f"europe-west1-docker.pkg.dev/{gcp.project}/ibdn")
    tag = os.getenv('IMAGE_TAG', 'latest')
    env_vars = [
        f"PROJECT_HOME={gcp.repo}",
        f"GCP_PROJECT={gcp.project}",
        f"GCP_ZONE={gcp.zone}",
        "MINIO_ROOT_USER=admin",
        "MINIO_ROOT_PASSWORD=password",
        "MINIO_ENDPOINT=http://minio:9000",
        "SPARK_MASTER_URL=spark://spark-manager:7077",
        "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "KAFKA_LOCAL_BOOTSTRAP_SERVERS=localhost:9092",
        "FLASK_PORT=5001",
        "MLFLOW_PORT=5003",
        "MLFLOW_TRACKING_URI=http://mlflow:5000",
        "MINIO_API_PORT=9000",
        "MINIO_CONSOLE_PORT=9001",
        "SPARK_MASTER_UI_PORT=8080",
        "SPARK_MASTER_PORT=7077",
        "KAFKA_PORT=9092",
        "KAFKA_TOPIC=flight-delay-ml-request",
        "KAFKA_RESPONSE_TOPIC=flight-delay-ml-response",
        "KAFKA_STATUS_TOPIC=flight-delay-ml-status",
        "DB_MODE=cassandra",
        "SPARK_CONTAINER=spark-manager",
        "KAFKA_CONTAINER=kafka",
        "AIRFLOW_UI_PORT=8085",
        "DEPLOY_MODE=gcloud",
        "MONGODB_URI=mongodb://mongodb:27017",
        "MONGODB_DATABASE=agile_data_science",
        f"ARTIFACT_REGISTRY={registry}",
        f"IMAGE_TAG={tag}",
    ]
    lines = [f"echo '{v}' >> {gcp.repo}/.env" for v in env_vars]
    lines[0] = lines[0].replace(">>", ">")
    cmd = " && ".join(lines)
    return _ssh_cmd(gcp, cmd)

def deploy_env(gcp):
    _check_or_fail(_ssh_deploy_env(gcp), "deploy_env")

@sh
def _ssh_docker_pull(gcp, db_mode):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose --profile db_{db_mode} pull")

def deploy_pull(gcp, db_mode):
    _check_or_fail(_ssh_docker_pull(gcp, db_mode), "docker pull")

@sh
def _ssh_docker_build(gcp, db_mode):
    registry = os.getenv('ARTIFACT_REGISTRY', f"europe-west1-docker.pkg.dev/{gcp.project}/ibdn")
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose --profile db_{db_mode} build --build-arg BASE_IMAGE={registry}/spark-base:4.1.1")

def deploy_build(gcp, db_mode):
    _check_or_fail(_ssh_docker_build(gcp, db_mode), "docker build")

@sh
def _ssh_docker_up(gcp, db_mode):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose --profile db_{db_mode} up -d")

def deploy_up(gcp, db_mode):
    _check_or_fail(_ssh_docker_up(gcp, db_mode), "docker up")

def deploy_compose(gcp, db_mode):
    registry = os.getenv('ARTIFACT_REGISTRY', f"europe-west1-docker.pkg.dev/{gcp.project}/ibdn")
    _check_or_fail(_ssh_docker_auth(gcp, registry), "docker auth")
    deploy_pull(gcp, db_mode)
    deploy_build(gcp, db_mode)
    deploy_up(gcp, db_mode)

@sh
def _ssh_docker_cloud_up(gcp, db_mode):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose -f docker-compose.cloud.yaml --profile db_{db_mode} up -d")

@sh
def _ssh_docker_auth(gcp, registry):
    return _ssh_cmd(gcp, f"gcloud auth configure-docker {registry.split('/')[0]} --quiet")


def deploy_cloud_compose(gcp, db_mode):
    registry = os.getenv('ARTIFACT_REGISTRY', f"europe-west1-docker.pkg.dev/{gcp.project}/ibdn")
    _check_or_fail(_ssh_docker_auth(gcp, registry), "docker auth")
    _check_or_fail(_ssh_docker_cloud_up(gcp, db_mode), "docker compose cloud up")

@sh
def _ssh_docker_down(gcp, db_mode):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose --profile db_{db_mode} down 2>/dev/null; true")

def deploy_down(gcp, db_mode):
    _ssh_docker_down(gcp, db_mode)

@sh
def _ssh_mkdir_jar(gcp):
    return _ssh_cmd(gcp, f"mkdir -p {gcp.repo}/flight_prediction/target/scala-2.13")

@sh
def _gcloud_scp_jar(gcp, local_jar):
    return (
        f"gcloud compute scp {local_jar} "
        f"{gcp.user}@{gcp.instance}:{gcp.repo}/flight_prediction/target/scala-2.13/ "
        f"--zone {gcp.zone} --quiet --project {gcp.project}"
    )

def deploy_jar(gcp, local_jar):
    _ssh_mkdir_jar(gcp)
    r = _gcloud_scp_jar(gcp, local_jar)
    _check_or_fail(r, "SCP JAR")

@sh
def _ssh_run_cmd(gcp, cmd):
    return _ssh_cmd(gcp, cmd)

def _progress_cmd(step, status, message=""):
    flk_port = os.getenv('FLASK_PORT', '5001')
    import json as _json
    body = _json.dumps({"step": step, "status": status, "message": message})
    data_arg = f"-d '{body}'"
    url = f"http://localhost:{flk_port}/api/pipeline/progress"
    # Try docker exec first (runs inside Flask container, always works if Flask is up)
    # Fallback to direct curl from host (works if Docker port mapping works)
    docker_cmd = f"docker exec flask curl -s --connect-timeout 3 --max-time 5 -X POST {url} -H 'Content-Type: application/json' {data_arg} >/dev/null 2>&1"
    direct_cmd = f"curl -s --connect-timeout 3 --max-time 5 -X POST {url} -H 'Content-Type: application/json' {data_arg} >/dev/null 2>&1"
    return f"({docker_cmd} || {direct_cmd}) || true"

def _send_progress(gcp, step, status, message=""):
    _ssh_run_cmd(gcp, _progress_cmd(step, status, message))

def _wrap_cmd(cmd, step, status, message=""):
    return f"{_progress_cmd(step, status, message)} && {cmd} && {_progress_cmd(step, 'done', message)}"

def run_pipeline(gcp, db_mode, cfg):
    env = f"DB_MODE={db_mode}"
    flk_port = os.getenv('FLASK_PORT', '5001')

    # Wait for Flask to be reachable before sending progress
    for _ in range(20):
        r = _ssh_run_cmd(gcp, f"docker exec flask curl -s --connect-timeout 2 --max-time 3 http://localhost:{flk_port}/api/pipeline/progress >/dev/null 2>&1 && echo OK || true")
        if r.returncode == 0 and 'OK' in r.stdout:
            break

    _check_or_fail(_ssh_run_cmd(gcp, _wrap_cmd(
        f"cd {gcp.repo} && {env} python3 scripts/create_bucket.py",
        "buckets", "running", "Creating MinIO buckets")), "Pipeline step")

    _check_or_fail(_ssh_run_cmd(gcp, _wrap_cmd(
        f"cd {gcp.repo} && {env} python3 scripts/download_data.py",
        "download", "running", "Downloading flight data")), "Pipeline step")

    _check_or_fail(_ssh_run_cmd(gcp, f"cd {gcp.repo} && docker exec minio mc alias set local http://localhost:9000 {cfg.minio_access_key} {cfg.minio_secret_key} 2>/dev/null; true"), "Pipeline step")
    data_files = [
        ("data/simple_flight_delay_features.jsonl.bz2", "lakehouse/raw"),
        ("data/origin_dest_distances.jsonl", "lakehouse/raw"),
    ]
    for local_path, bucket in data_files:
        fname = local_path.split('/')[-1]
        _check_or_fail(_ssh_run_cmd(gcp, _wrap_cmd(
            f"cd {gcp.repo} && docker cp {local_path} minio:/tmp/ && docker exec minio mc cp /tmp/{fname} local/{bucket}/",
            "upload", "done", "Data uploaded to MinIO")), "Pipeline step")

    _check_or_fail(_ssh_run_cmd(gcp, _wrap_cmd(
        f"cd {gcp.repo} && {env} python3 scripts/import_distances.py",
        "import_distances", "running", "Importing distance data")), "Pipeline step")

    topics_cmd = " && ".join([
        f"cd {gcp.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-request --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
        f"cd {gcp.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-response --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
        f"cd {gcp.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-status --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
    ])
    _check_or_fail(_ssh_run_cmd(gcp, _wrap_cmd(
        topics_cmd,
        "topics", "running", "Creating Kafka topics")), "Pipeline step")

@sh
def _ssh_start_prediction(gcp, cfg):
    return _ssh_cmd(gcp,
        f"cd {gcp.repo} && docker exec -d spark-manager spark-submit "
        f"--master {cfg.spark_master} "
        f"--deploy-mode cluster "
        f"--conf spark.cores.max=2 "
        f"{_s3a_flags_remote(cfg)} "
        f"--class es.upm.dit.ging.predictor.MakePrediction "
        f"/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
    )

def start_prediction(gcp, cfg):
    _check_or_fail(_ssh_start_prediction(gcp, cfg), "start_prediction")

def _s3a_flags_remote(cfg):
    return (
        f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
        f"--conf spark.hadoop.fs.s3a.access.key={cfg.minio_access_key} "
        f"--conf spark.hadoop.fs.s3a.secret.key={cfg.minio_secret_key} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
        f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

# ---- Tunnel ----
@sh(timeout=60)
def _iap_enable_api(gcp):
    return f"gcloud services enable iap.googleapis.com --project {gcp.project}"

@sh(timeout=10)
def _iap_list_firewall(gcp):
    return (
        f"gcloud compute firewall-rules list "
        f"--filter=name=allow-iap --project {gcp.project} "
        f"--format=value(name)"
    )

@sh(timeout=30)
def _iap_create_firewall(gcp):
    return (
        f"gcloud compute firewall-rules create allow-iap "
        f"--direction=INGRESS --priority=1000 "
        f"--network=default --action=ALLOW "
        f"--rules=tcp:5001,tcp:5003 "
        f"--source-ranges=35.235.240.0/20 "
        f"--project {gcp.project} 2>/dev/null; true"
    )

def ensure_iap(gcp):
    logging.info("Enabling IAP API...")
    _iap_enable_api(gcp)
    logging.info("Creating IAP firewall rule...")
    _iap_create_firewall(gcp)

def tunnel(gcp):
    from rich.console import Console
    console = Console()
    ports = [5001, 5003, 8085, 9001, 8081]
    labels = ["Flask", "MLflow", "Airflow", "MinIO", "Spark"]
    forward_args = []
    for port in ports:
        forward_args.extend(["-L", f"{port}:localhost:{port}"])

    for attempt in range(3):
        console.print(f"[dim]Starting tunnels ({attempt+1}/3)...[/dim]")
        cmd = [
            "gcloud", "compute", "ssh",
            f"{gcp.user}@{gcp.instance}",
            "--zone", gcp.zone, "--project", gcp.project,
            "--quiet", "--", "-N"
        ] + forward_args
        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(8)

        import socket
        all_ok = True
        for port in ports:
            try:
                s = socket.create_connection(('localhost', port), timeout=2)
                s.close()
            except Exception:
                all_ok = False
                break

        if all_ok:
            console.print(f"[dim]Tunnels: ✓ ({', '.join(labels)})[/dim]")
            try:
                while True:
                    time.sleep(5)
                    if p.poll() is not None:
                        console.print(f"[yellow]Tunnels lost. Reconnecting...[/yellow]")
                        break
            except KeyboardInterrupt:
                console.print("[dim]Stopping tunnels...[/dim]")
                p.terminate()
                try:
                    p.wait(timeout=5)
                except Exception:
                    p.kill()
                raise
        else:
            console.print(f"[yellow]Tunnels failed to connect, retry {attempt+1}/3[/yellow]")
        p.terminate()
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()

    console.print("[red]Tunnels: ✗ Could not maintain connection.[/red]")
    console.print(f"[dim]Reconnect:[/dim] [bold]predict gcloud --db cassandra[/bold]")

def suggest_tunnel():
    from rich.console import Console
    from rich.panel import Panel
    console = Console()
    console.print()
    console.print(Panel(
        "  [bold]GKE Deployment complete![/bold]\n\n"
        "  [dim]Access services locally:[/dim]\n"
        "  [bold]kubectl port-forward -n ibdn svc/flask 5001:5001 &[/bold]\n"
        "  [bold]kubectl port-forward -n ibdn svc/airflow-webserver 8085:8080 &[/bold]\n"
        "  [bold]kubectl port-forward -n ibdn svc/mlflow 5003:5000 &[/bold]\n"
        "  [bold]kubectl port-forward -n ibdn svc/minio 9001:9001 &[/bold]\n"
        "  [bold]kubectl port-forward -n ibdn svc/spark-manager 8081:8080 &[/bold]\n\n"
        "  [dim]Then open http://localhost:5001[/dim]",
        border_style="green", expand=False
    ))


def port_forward_gke():
    from rich.console import Console
    console = Console()
    forwards = [
        ("flask", 5001, 5001),
        ("airflow-webserver", 8085, 8080),
        ("mlflow", 5003, 5000),
        ("minio", 9001, 9001),
        ("spark-manager", 8081, 8080),
    ]
    procs = []
    for svc, local, remote in forwards:
        p = subprocess.Popen(
            ["kubectl", "port-forward", "-n", "ibdn", f"svc/{svc}", f"{local}:{remote}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        procs.append(p)
        time.sleep(2)
    console.print(f"[dim]Port forwards: ✓ (Flask, MLflow, Airflow, MinIO, Spark)[/dim]")
    console.print(f"[dim]Open [link=http://localhost:5001]http://localhost:5001[/link][/dim]")
    console.print("[dim]Press Ctrl+C to stop[/dim]")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        console.print("[dim]Stopping port forwards...[/dim]")
    finally:
        for p in procs:
            p.terminate()

# ---- K8S (GKE) ----
@sh
def _gcloud_create_gke_cluster(gcp):
    machine_type = os.getenv('GKE_MACHINE_TYPE', 'e2-standard-4')
    num_nodes = os.getenv('GKE_NUM_NODES', '4')
    cluster_name = os.getenv('GKE_CLUSTER_NAME', 'ibdn-cluster')
    return (
        f"gcloud container --project {gcp.project} "
        f"clusters create {cluster_name} "
        f"--zone {gcp.zone} --num-nodes={num_nodes} "
        f"--machine-type={machine_type} --disk-size=30"
    )

def create_gke_cluster(gcp):
    logging.info("Creating GKE cluster...")
    r = _gcloud_create_gke_cluster(gcp)
    if r.returncode != 0:
        if 'Already exists' in r.stderr:
            logging.info("GKE cluster already exists, skipping creation")
            return
        raise RuntimeError(f"Failed to create GKE cluster: {r.stderr.strip()}")

@sh(check=True)
def get_gke_credentials(gcp):
    cluster_name = os.getenv('GKE_CLUSTER_NAME', 'ibdn-cluster')
    return (
        f"gcloud container clusters get-credentials {cluster_name} "
        f"--zone {gcp.zone} --project {gcp.project}"
    )

@sh(check=True)
def _wait_for_gke_cluster(gcp):
    cluster_name = os.getenv('GKE_CLUSTER_NAME', 'ibdn-cluster')
    return (
        f"gcloud container clusters list --filter='name={cluster_name} "
        f"AND status=RUNNING' --format='value(name)'"
    )

def wait_for_gke_cluster(gcp):
    import time
    logging.info("Waiting for GKE cluster to be ready...")
    for _ in range(30):
        r = _wait_for_gke_cluster(gcp)
        if r.stdout.strip():
            logging.info("GKE cluster is ready")
            return
        time.sleep(10)
    raise RuntimeError("GKE cluster did not become ready within 5 minutes")

def build_and_push_images(cfg):
    registry = os.getenv('ARTIFACT_REGISTRY', f"europe-west1-docker.pkg.dev/{os.getenv('GCP_PROJECT', '')}/ibdn")
    tag = os.getenv('IMAGE_TAG', 'latest')

    logging.info(f"Building and pushing images to {registry} via Cloud Build")

    images = [
        ("spark", "docker/dockerfile.spark"),
        ("flask", "docker/dockerfile.python"),
        ("airflow", "docker/dockerfile.airflow"),
    ]

    for name, dockerfile in images:
        image = f"{registry}/{name}:{tag}"
        logging.info(f"Building and pushing {image}...")
        steps = f"""
steps:
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', '{image}', '-f', '{dockerfile}'
"""
        if name == "spark":
            steps += f",  '--build-arg', 'BASE_IMAGE={registry}/spark-base:4.1.1'"
        steps += f""",  '.']
images: ['{image}']
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(steps)
            config_path = f.name
        subprocess.run(
            f"gcloud builds submit --config={config_path} --machine-type=e2-highcpu-8 .",
            shell=True, check=True, cwd=cfg.project_home
        )
        os.unlink(config_path)

    logging.info("Images built and pushed")


def substitute_image_refs(cfg):
    project = os.getenv('GCP_PROJECT', '')
    k8s_dir = os.path.join(CLOUD_DIR, "k8s")
    # Use a temp directory to avoid modifying source files
    import tempfile
    tmp_manifests = tempfile.mkdtemp()
    for root, _, files in os.walk(k8s_dir):
        for f in files:
            if f.endswith(('.yaml', '.yml')):
                src = os.path.join(root, f)
                with open(src) as fh:
                    content = fh.read().replace('REPLACE_PROJECT', project)
                dst = os.path.join(tmp_manifests, f)
                with open(dst, 'w') as fh:
                    fh.write(content)
    return tmp_manifests

@sh
def _kubectl_apply_k8s(manifest_dir):
    return f"kubectl apply -f {manifest_dir} --validate=false"


def deploy_k8s(gcp):
    k8s_dir = os.path.join(CLOUD_DIR, "k8s")
    if not os.path.isdir(k8s_dir):
        logging.info("No k8s/ directory found.")
        return
    if subprocess.run("which gke-gcloud-auth-plugin", shell=True, capture_output=True).returncode != 0:
        logging.info("Installing gke-gcloud-auth-plugin...")
        subprocess.run("gcloud components install gke-gcloud-auth-plugin --quiet",
                      shell=True, check=True)
    manifest_dir = substitute_image_refs(gcp)
    for attempt in range(3):
        r = _kubectl_apply_k8s(manifest_dir)
        if r.returncode == 0:
            break
        logging.warning(f"kubectl apply failed (attempt {attempt+1}/3), retrying...")
        time.sleep(10)
    else:
        raise RuntimeError("kubectl apply failed after 3 attempts")
    import shutil
    shutil.rmtree(manifest_dir, ignore_errors=True)
    logging.info("Waiting for pods to be ready...")
    subprocess.run(
        "kubectl wait --for=condition=ready pod -l app=flask -n ibdn --timeout=300s",
        shell=True, capture_output=True
    )

@sh
def _gcloud_delete_gke_cluster(gcp):
    cluster_name = os.getenv('GKE_CLUSTER_NAME', 'ibdn-cluster')
    return (
        f"gcloud container clusters delete {cluster_name} "
        f"--zone {gcp.zone} --project {gcp.project} --quiet"
    )

def delete_gke_cluster(gcp):
    logging.info("Deleting GKE cluster...")
    _gcloud_delete_gke_cluster(gcp)


@sh
def _gcloud_resize_gke_cluster(gcp, nodes):
    cluster_name = os.getenv('GKE_CLUSTER_NAME', 'ibdn-cluster')
    pool_name = "default-pool"
    r = subprocess.run(
        f"gcloud container node-pools list --cluster {cluster_name} "
        f"--zone {gcp.zone} --project {gcp.project} --format=value(name)",
        shell=True, capture_output=True, text=True
    )
    if r.stdout.strip():
        pool_name = r.stdout.strip().split('\n')[0]
    return (
        f"gcloud container clusters resize {cluster_name} "
        f"--node-pool {pool_name} --num-nodes {nodes} "
        f"--zone {gcp.zone} --project {gcp.project} --quiet"
    )


def scale_gke_cluster(gcp, nodes):
    logging.info(f"Scaling GKE cluster to {nodes} nodes...")
    _gcloud_resize_gke_cluster(gcp, nodes)