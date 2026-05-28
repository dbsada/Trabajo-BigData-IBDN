import os, time, subprocess, logging
from rich.console import Console
from rich.panel import Panel
from utils.shell import sh

_log_console = Console()

def log(msg):
    logging.info(msg)

class GCPOrchestrator:
    def __init__(self, mode="gcloud", db="cassandra"):
        self.mode = mode
        self.db = db
        self.project = os.getenv("GCP_PROJECT") or self._detect_project()
        self.zone = os.getenv("GCP_ZONE", "europe-west1-b")
        self.instance = os.getenv("GCP_INSTANCE", "bigdata-vm")
        self.user = os.getenv("GCP_USER", "ubuntu")
        self.repo = os.getenv("REMOTE_REPO", "~/ibdn")
        self.access_key = os.getenv("MINIO_ROOT_USER", "admin")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")

    def _gcloud_cmd(self, *args):
        return f"gcloud compute --project {self.project} {' '.join(args)}"

    def _ssh_cmd(self, command):
        return (
            f"gcloud compute ssh {self.user}@{self.instance} "
            f"--zone {self.zone} --command '{command}' --quiet "
            f"--project {self.project}"
        )

    def _detect_project(self):
        try:
            r = subprocess.run(
                ["gcloud", "config", "get-value", "project"],
                capture_output=True, text=True, check=True)
            return r.stdout.strip()
        except Exception:
            raise RuntimeError("GCP_PROJECT not set and could not detect from gcloud config. Set it in .env")

    def _show_error(self, title, detail):
        _log_console.print()
        lines = detail.strip().splitlines()
        if len(lines) > 30:
            shown = "...\n" + "\n".join(lines[-30:])
        else:
            shown = detail.strip()[:2000]
        _log_console.print(Panel(
            f"[red]{title}[/red]\n\n[dim]{shown}[/dim]",
            border_style="red", expand=False
        ))

    def _check_or_fail(self, r, label=""):
        if r.returncode != 0:
            logging.error(f"{label} failed (exit {r.returncode})\nFull stderr saved below:\n{r.stderr.strip()}")
            self._show_error(f"{label} falló", r.stderr)
            raise RuntimeError(f"{label} (exit {r.returncode})")
        return r

    # ---- VM Control ----

    @sh
    def _gcloud_describe_instance(self):
        return self._gcloud_cmd("instances", "describe", self.instance, "--zone", self.zone)

    def vm_exists(self):
        return self._gcloud_describe_instance().returncode == 0

    @sh
    def _gcloud_create_vm(self):
        cloud_init = os.path.join(os.path.dirname(__file__), "cloud-init.yaml")
        return (
            f"gcloud compute instances create {self.instance} "
            f"--zone {self.zone} --machine-type=e2-standard-4 "
            f"--image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud "
            f"--boot-disk-size=50 "
            f"--metadata-from-file user-data={cloud_init} "
            f"--project {self.project}"
        )

    def create_vm(self):
        log("Creating VM...")
        r = self._gcloud_create_vm()
        if r.returncode != 0:
            logging.error(f"VM creation failed (exit {r.returncode})\n{r.stderr.strip()}")
            self._show_error("VM creation falló", r.stderr)
            raise RuntimeError(f"VM creation failed (exit {r.returncode})")
        log("VM created.")

    @sh(check=True)
    def start_vm(self):
        return self._gcloud_cmd("instances", "start", self.instance, "--zone", self.zone)

    @sh(check=True)
    def stop_vm(self):
        return self._gcloud_cmd("instances", "stop", self.instance, "--zone", self.zone)

    @sh
    def _ssh_echo_ready(self):
        return self._ssh_cmd("echo ready")

    @sh
    def _ssh_docker_compose_version(self):
        return self._ssh_cmd("docker compose version")

    def wait_for_vm(self, timeout=180):
        for _ in range(timeout):
            if self._ssh_echo_ready().returncode == 0:
                for _ in range(timeout // 2):
                    if self._ssh_docker_compose_version().returncode == 0:
                        return
                    time.sleep(2)
                raise TimeoutError("Docker compose not ready after provisioning")
            time.sleep(2)
        raise TimeoutError("VM not ready after %ds" % timeout)

    @sh(check=True)
    def _gcloud_get_external_ip(self):
        return (
            f"gcloud compute instances describe {self.instance} "
            f"--zone {self.zone} "
            f"--format=value(networkInterfaces[0].accessConfigs[0].natIP) "
            f"--project {self.project}"
        )

    def get_external_ip(self):
        return self._gcloud_get_external_ip().stdout.strip()

    # ---- Deploy ----

    @sh
    def _ssh_check_repo(self):
        return self._ssh_cmd(f"test -d {self.repo}/.git")

    @sh
    def _ssh_git_pull(self):
        return self._ssh_cmd(f"cd {self.repo} && git pull")

    @sh
    def _ssh_git_clone(self):
        github_repo = os.getenv("GITHUB_REPO")
        if not github_repo:
            raise RuntimeError("GITHUB_REPO not set in .env")
        return self._ssh_cmd(f"git clone {github_repo} {self.repo}")

    def deploy_code(self):
        r = self._ssh_check_repo()
        if r.returncode == 0:
            log("Pulling latest code...")
            self._check_or_fail(self._ssh_git_pull(), "git pull")
        else:
            log("Cloning repository...")
            self._check_or_fail(self._ssh_git_clone(), "git clone")

    @sh
    def _ssh_deploy_env(self):
        return self._ssh_cmd(f"cp {self.repo}/.env.example {self.repo}/.env")

    def deploy_env(self):
        self._check_or_fail(self._ssh_deploy_env(), "deploy_env")

    @sh
    def _ssh_docker_pull(self):
        return self._ssh_cmd(f"cd {self.repo} && docker compose --profile db_{self.db} pull")

    def deploy_pull(self):
        self._check_or_fail(self._ssh_docker_pull(), "docker pull")

    @sh
    def _ssh_docker_build(self):
        return self._ssh_cmd(f"cd {self.repo} && docker compose --profile db_{self.db} build")

    def deploy_build(self):
        self._check_or_fail(self._ssh_docker_build(), "docker build")

    @sh
    def _ssh_docker_up(self):
        return self._ssh_cmd(f"cd {self.repo} && docker compose --profile db_{self.db} up -d")

    def deploy_up(self):
        self._check_or_fail(self._ssh_docker_up(), "docker up")

    def deploy_compose(self):
        self.deploy_pull()
        self.deploy_build()
        self.deploy_up()

    @sh
    def _ssh_docker_down(self):
        return self._ssh_cmd(f"cd {self.repo} && docker compose --profile db_{self.db} down 2>/dev/null; true")

    def deploy_down(self):
        self._ssh_docker_down()

    @sh
    def _ssh_mkdir_jar(self):
        return self._ssh_cmd(f"mkdir -p {self.repo}/flight_prediction/target/scala-2.13")

    @sh
    def _gcloud_scp_jar(self, local_jar):
        return (
            f"gcloud compute scp {local_jar} "
            f"{self.user}@{self.instance}:{self.repo}/flight_prediction/target/scala-2.13/ "
            f"--zone {self.zone} --quiet --project {self.project}"
        )

    def deploy_jar(self, local_jar):
        self._ssh_mkdir_jar()
        r = self._gcloud_scp_jar(local_jar)
        if r.returncode != 0:
            raise RuntimeError(f"SCP JAR failed: {r.stderr.strip()}")

    @sh
    def _ssh_run_pipeline_cmd(self, cmd):
        return self._ssh_cmd(cmd)

    def run_pipeline(self):
        env = f"DB_MODE={self.db}"
        commands = [
            f"cd {self.repo} && {env} python3 scripts/create_bucket.py",
            f"cd {self.repo} && {env} python3 scripts/download_data.py",
            f"cd {self.repo} && docker exec minio mc alias set local http://localhost:9000 {self.access_key} {self.secret_key} 2>/dev/null; true",
        ]
        data_files = [
            ("data/simple_flight_delay_features.jsonl.bz2", "lakehouse/raw"),
            ("data/origin_dest_distances.jsonl", "lakehouse/raw"),
        ]
        for local_path, bucket in data_files:
            fname = local_path.split('/')[-1]
            commands.append(
                f"cd {self.repo} && docker cp {local_path} minio:/tmp/ && docker exec minio mc cp /tmp/{fname} local/{bucket}/")
        commands += [
            f"cd {self.repo} && {env} python3 scripts/import_distances.py",
            f"cd {self.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-request --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
            f"cd {self.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-response --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
            f"cd {self.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-status --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
        ]
        for cmd in commands:
            self._check_or_fail(self._ssh_run_pipeline_cmd(cmd), "Pipeline step")

    @sh
    def start_prediction(self):
        return self._ssh_cmd(
            f"cd {self.repo} && docker exec -d spark spark-submit "
            f"--master spark://spark:7077 "
            f"--deploy-mode cluster "
            f"--conf spark.cores.max=2 "
            f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            f"--conf spark.hadoop.fs.s3a.access.key={self.access_key} "
            f"--conf spark.hadoop.fs.s3a.secret.key={self.secret_key} "
            f"--conf spark.hadoop.fs.s3a.path.style.access=true "
            f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            f"--class es.upm.dit.ging.predictor.MakePrediction "
            f"/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
        )

    # ---- Tunnel ----

    @sh(timeout=60)
    def _iap_enable_api(self):
        return f"gcloud services enable iap.googleapis.com --project {self.project}"

    @sh(timeout=10)
    def _iap_list_firewall(self):
        return (
            f"gcloud compute firewall-rules list "
            f"--filter=name=allow-iap --project {self.project} "
            f"--format=value(name)"
        )

    @sh(timeout=30, check=True)
    def _iap_create_firewall(self):
        return (
            f"gcloud compute firewall-rules create allow-iap "
            f"--direction=INGRESS --priority=1000 "
            f"--network=default --action=ALLOW "
            f"--rules=tcp:5001,tcp:5002 "
            f"--source-ranges=35.235.240.0/20 "
            f"--project {self.project}"
        )

    def ensure_iap(self):
        log("Enabling IAP API...")
        self._iap_enable_api()
        rules = self._iap_list_firewall()
        if "allow-iap" not in rules.stdout:
            log("Creating IAP firewall rule...")
            self._iap_create_firewall()
            log("IAP firewall rule created")

    def tunnel(self):
        log("Starting IAP tunnels...")
        procs = []
        ports = [5001, 5002]
        os.makedirs("logs", exist_ok=True)
        try:
            for port in ports:
                cmd = [
                    "gcloud", "compute", "start-iap-tunnel",
                    self.instance, str(port),
                    "--local-host-port", f"localhost:{port}",
                    "--zone", self.zone, "--project", self.project,
                ]
                log_file = open(f"logs/iap_tunnel_{port}.log", "w")
                p = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
                procs.append(p)
                time.sleep(3)
            log(f"IAP tunnels ready: localhost:{ports[0]}, localhost:{ports[1]}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log("Stopping tunnels...")
        finally:
            for p in procs:
                p.terminate()
                try: p.wait(timeout=5)
                except: p.kill()

    @staticmethod
    def suggest_tunnel():
        _log_console.print()
        _log_console.print(Panel(
            "  [bold]Deployment complete![/bold]\n"
            "  [dim]Flask UI:[/dim] http://localhost:5001\n"
            "  [dim]MLflow:[/dim]  http://localhost:5002\n\n"
            "  Access via IAP (GCloud auth required). No ports exposed publicly.",
            border_style="green", expand=False
        ))

    # ---- K8S (GKE) ----

    def create_gke_cluster(self):
        log("Creating GKE cluster...")
        self._gcloud_create_gke_cluster()

    @sh(check=True)
    def _gcloud_create_gke_cluster(self):
        return (
            f"gcloud container --project {self.project} "
            f"clusters create ibdn-cluster "
            f"--zone {self.zone} --num-nodes=3 "
            f"--machine-type=e2-small --disk-size=30"
        )

    @sh(check=True)
    def _kubectl_apply_k8s(self):
        k8s_dir = os.path.join(os.path.dirname(__file__), "k8s")
        return f"kubectl apply -f {k8s_dir}"

    def deploy_k8s(self):
        k8s_dir = os.path.join(os.path.dirname(__file__), "k8s")
        if not os.path.isdir(k8s_dir):
            log("No k8s/ directory found.")
            return
        self._kubectl_apply_k8s()

    # ---- Full Flow ----

    def run(self):
        if self.mode == "gcloud":
            try:
                if not self.vm_exists():
                    log("VM not found, creating...")
                    self.create_vm()
                    self.wait_for_vm(timeout=180)
                else:
                    self.start_vm()
                    self.wait_for_vm(timeout=120)
                self.deploy_code()
                self.deploy_env()
                self.deploy_compose()
                self.run_pipeline()
                self.start_prediction()
                self.suggest_tunnel()
                self.ensure_iap()
                log("Opening IAP tunnels (Ctrl+C to stop and shut down VM)...")
                self.tunnel()
            except KeyboardInterrupt:
                log("Interrupted.")
            finally:
                self.stop_vm()

        elif self.mode == "gke":
            self.create_gke_cluster()
            self.deploy_k8s()
            self.suggest_tunnel()
