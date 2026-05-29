import os
import time
import subprocess
import logging
from utils.shell import sh
from cloud.config import DeployConfig


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
    return (
        f"gcloud compute ssh {gcp.user}@{gcp.instance} "
        f"--zone {gcp.zone} --command '{command}' --quiet "
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
    cloud_init = os.path.join(os.path.dirname(__file__), "cloud-init.yaml")
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


@sh(check=True)
def _gcloud_get_external_ip(gcp):
    return (
        f"gcloud compute instances describe {gcp.instance} "
        f"--zone {gcp.zone} "
        f"--format=value(networkInterfaces[0].accessConfigs[0].natIP) "
        f"--project {gcp.project}"
    )


def get_external_ip(gcp):
    return _gcloud_get_external_ip(gcp).stdout.strip()


# ---- Deploy ----

@sh
def _ssh_check_repo(gcp):
    return _ssh_cmd(gcp, f"test -d {gcp.repo}/.git")


@sh
def _ssh_git_pull(gcp):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && git pull")


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
    return _ssh_cmd(gcp, f"cp {gcp.repo}/.env.example {gcp.repo}/.env")


def deploy_env(gcp):
    _check_or_fail(_ssh_deploy_env(gcp), "deploy_env")


@sh
def _ssh_docker_pull(gcp, db_mode):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose --profile db_{db_mode} pull")


def deploy_pull(gcp, db_mode):
    _check_or_fail(_ssh_docker_pull(gcp, db_mode), "docker pull")


@sh
def _ssh_docker_build(gcp, db_mode):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose --profile db_{db_mode} build")


def deploy_build(gcp, db_mode):
    _check_or_fail(_ssh_docker_build(gcp, db_mode), "docker build")


@sh
def _ssh_docker_up(gcp, db_mode):
    return _ssh_cmd(gcp, f"cd {gcp.repo} && docker compose --profile db_{db_mode} up -d")


def deploy_up(gcp, db_mode):
    _check_or_fail(_ssh_docker_up(gcp, db_mode), "docker up")


def deploy_compose(gcp, db_mode):
    deploy_pull(gcp, db_mode)
    deploy_build(gcp, db_mode)
    deploy_up(gcp, db_mode)


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


def run_pipeline(gcp, db_mode, cfg):
    env = f"DB_MODE={db_mode}"
    commands = [
        f"cd {gcp.repo} && {env} python3 scripts/create_bucket.py",
        f"cd {gcp.repo} && {env} python3 scripts/download_data.py",
        f"cd {gcp.repo} && docker exec minio mc alias set local http://localhost:9000 {cfg.minio_access_key} {cfg.minio_secret_key} 2>/dev/null; true",
    ]
    data_files = [
        ("data/simple_flight_delay_features.jsonl.bz2", "lakehouse/raw"),
        ("data/origin_dest_distances.jsonl", "lakehouse/raw"),
    ]
    for local_path, bucket in data_files:
        fname = local_path.split('/')[-1]
        commands.append(
            f"cd {gcp.repo} && docker cp {local_path} minio:/tmp/ && docker exec minio mc cp /tmp/{fname} local/{bucket}/")
    commands += [
        f"cd {gcp.repo} && {env} python3 scripts/import_distances.py",
        f"cd {gcp.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-request --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
        f"cd {gcp.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-response --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
        f"cd {gcp.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-status --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
    ]
    for cmd in commands:
        _check_or_fail(_ssh_run_cmd(gcp, cmd), "Pipeline step")


def start_prediction(gcp, cfg):
    cmd = _ssh_cmd(gcp,
        f"cd {gcp.repo} && docker exec -d spark-manager spark-submit "
        f"--master {cfg.spark_master} "
        f"--deploy-mode cluster "
        f"--conf spark.cores.max=2 "
        f"{_s3a_flags_remote(cfg)} "
        f"--class es.upm.dit.ging.predictor.MakePrediction "
        f"/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
    )
    _check_or_fail(cmd, "start_prediction")


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


@sh(timeout=30, check=True)
def _iap_create_firewall(gcp):
    return (
        f"gcloud compute firewall-rules create allow-iap "
        f"--direction=INGRESS --priority=1000 "
        f"--network=default --action=ALLOW "
        f"--rules=tcp:5001,tcp:5002 "
        f"--source-ranges=35.235.240.0/20 "
        f"--project {gcp.project}"
    )


def ensure_iap(gcp):
    logging.info("Enabling IAP API...")
    _iap_enable_api(gcp)
    rules = _iap_list_firewall(gcp)
    if "allow-iap" not in rules.stdout:
        logging.info("Creating IAP firewall rule...")
        _iap_create_firewall(gcp)
        logging.info("IAP firewall rule created")


def tunnel(gcp):
    logging.info("Starting IAP tunnels...")
    procs = []
    ports = [5001, 5002]
    os.makedirs("logs", exist_ok=True)
    try:
        for port in ports:
            cmd = [
                "gcloud", "compute", "start-iap-tunnel",
                gcp.instance, str(port),
                "--local-host-port", f"localhost:{port}",
                "--zone", gcp.zone, "--project", gcp.project,
            ]
            log_file = open(f"logs/iap_tunnel_{port}.log", "w")
            p = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
            procs.append(p)
            time.sleep(3)
        logging.info(f"IAP tunnels ready: localhost:{ports[0]}, localhost:{ports[1]}")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping tunnels...")
    finally:
        for p in procs:
            p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def suggest_tunnel():
    from rich.console import Console
    from rich.panel import Panel
    console = Console()
    console.print()
    console.print(Panel(
        "  [bold]Deployment complete![/bold]\n"
        "  [dim]Flask UI:[/dim] http://localhost:5001\n"
        "  [dim]MLflow:[/dim]  http://localhost:5002\n\n"
        "  Access via IAP (GCloud auth required). No ports exposed publicly.",
        border_style="green", expand=False
    ))


# ---- K8S (GKE) ----

@sh(check=True)
def _gcloud_create_gke_cluster(gcp):
    return (
        f"gcloud container --project {gcp.project} "
        f"clusters create ibdn-cluster "
        f"--zone {gcp.zone} --num-nodes=3 "
        f"--machine-type=e2-small --disk-size=30"
    )


def create_gke_cluster(gcp):
    logging.info("Creating GKE cluster...")
    _gcloud_create_gke_cluster(gcp)


@sh(check=True)
def _kubectl_apply_k8s():
    k8s_dir = os.path.join(os.path.dirname(__file__), "k8s")
    return f"kubectl apply -f {k8s_dir}"


def deploy_k8s(gcp):
    k8s_dir = os.path.join(os.path.dirname(__file__), "k8s")
    if not os.path.isdir(k8s_dir):
        logging.info("No k8s/ directory found.")
        return
    _kubectl_apply_k8s()
