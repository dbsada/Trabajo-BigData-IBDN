import os, time, subprocess, logging
from rich.console import Console
from rich.panel import Panel

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
        self._base = ["gcloud", "compute", "--project", self.project]

    def _detect_project(self):
        try:
            r = subprocess.run(["gcloud", "config", "get-value", "project"], capture_output=True, text=True, check=True)
            return r.stdout.strip()
        except Exception:
            raise RuntimeError("GCP_PROJECT not set and could not detect from gcloud config. Set it in .env")

    def _gcloud(self, *args, **kwargs):
        cmd = self._base + list(args)
        return subprocess.run(cmd, capture_output=True, text=True, **kwargs)

    def _ssh(self, command, **kwargs):
        kwargs.pop("check", None)
        cmd = self._base + [
            "ssh", f"{self.user}@{self.instance}",
            "--zone", self.zone, "--command", command, "--quiet",
        ]
        return subprocess.run(cmd, capture_output=True, text=True, **kwargs)

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

    def _ssh_or_fail(self, command, label=""):
        r = self._ssh(command)
        if r.returncode != 0:
            logging.error(f"{label} failed (exit {r.returncode})\nFull stderr saved below:\n{r.stderr.strip()}")
            self._show_error(f"{label} falló", r.stderr)
            raise RuntimeError(f"{label} (exit {r.returncode})")
        return r

    # ---- VM Control ----

    def vm_exists(self):
        r = self._gcloud("instances", "describe", self.instance, "--zone", self.zone)
        return r.returncode == 0

    def create_vm(self):
        log("Creating VM...")
        cloud_init = os.path.join(os.path.dirname(__file__), "cloud-init.yaml")
        cmd = self._base + [
            "instances", "create", self.instance,
            "--zone", self.zone,
            "--machine-type=e2-standard-4",
            "--image-family=ubuntu-2204-lts",
            "--image-project=ubuntu-os-cloud",
            "--boot-disk-size=50",
            "--metadata-from-file", f"user-data={cloud_init}",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            logging.error(f"VM creation failed (exit {r.returncode})\n{r.stderr.strip()}")
            self._show_error("VM creation falló", r.stderr)
            raise RuntimeError(f"VM creation failed (exit {r.returncode})")
        log("VM created.")

    def start_vm(self):
        self._gcloud("instances", "start", self.instance, "--zone", self.zone, check=True)

    def stop_vm(self):
        self._gcloud("instances", "stop", self.instance, "--zone", self.zone, check=True)

    def wait_for_vm(self, timeout=180):
        for _ in range(timeout):
            r = self._ssh("echo ready")
            if r.returncode == 0:
                # Wait for cloud-init + docker compose to be ready
                for _ in range(timeout // 2):
                    r = self._ssh("docker compose version")
                    if r.returncode == 0:
                        return
                    time.sleep(2)
                raise TimeoutError("Docker compose not ready after provisioning")
            time.sleep(2)
        raise TimeoutError("VM not ready after %ds" % timeout)

    def get_external_ip(self):
        r = self._gcloud(
            "instances", "describe", self.instance, "--zone", self.zone,
            "--format=value(networkInterfaces[0].accessConfigs[0].natIP)", check=True)
        return r.stdout.strip()

    # ---- Deploy ----

    def deploy_code(self):
        github_repo = os.getenv("GITHUB_REPO")
        if not github_repo:
            raise RuntimeError("GITHUB_REPO not set in .env")
        r = self._ssh(f"test -d {self.repo}/.git")
        if r.returncode == 0:
            log("Pulling latest code...")
            self._ssh_or_fail(f"cd {self.repo} && git pull", "git pull")
        else:
            log("Cloning repository...")
            self._ssh_or_fail(f"git clone {github_repo} {self.repo}", "git clone")

    def deploy_env(self):
        self._ssh_or_fail(f"cp {self.repo}/.env.example {self.repo}/.env", "deploy_env")

    def deploy_pull(self):
        self._ssh_or_fail(f"cd {self.repo} && docker compose --profile db_{self.db} pull", "docker pull")

    def deploy_build(self):
        self._ssh_or_fail(f"cd {self.repo} && docker compose --profile db_{self.db} build", "docker build")

    def deploy_up(self):
        self._ssh_or_fail(f"cd {self.repo} && docker compose --profile db_{self.db} up -d", "docker up")

    def deploy_compose(self):
        self.deploy_pull()
        self.deploy_build()
        self.deploy_up()

    def deploy_down(self):
        self._ssh(f"cd {self.repo} && docker compose --profile db_{self.db} down 2>/dev/null; true")

    def run_pipeline(self):
        env = f"DB_MODE={self.db}"
        commands = [
            f"cd {self.repo} && {env} python3 scripts/create_bucket.py",
            f"cd {self.repo} && {env} python3 scripts/download_data.py",
            # Configure mc alias and upload data to MinIO
            f"cd {self.repo} && docker exec minio mc alias set local http://localhost:9000 {self.access_key} {self.secret_key} 2>/dev/null; true",
        ]
        data_files = [
            ("data/simple_flight_delay_features.jsonl.bz2", "lakehouse/raw"),
            ("data/origin_dest_distances.jsonl", "lakehouse/raw"),
        ]
        for local_path, bucket in data_files:
            commands.append(
                f"cd {self.repo} && docker cp {local_path} minio:/tmp/ && docker exec minio mc cp /tmp/{local_path.split('/')[-1]} local/{bucket}/")
        commands += [
            f"cd {self.repo} && {env} python3 scripts/import_distances.py",
            # Create Kafka topics
            f"cd {self.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-request --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
            f"cd {self.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-response --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
            f"cd {self.repo} && docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic flight-delay-ml-status --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null; true",
        ]
        for cmd in commands:
            self._ssh_or_fail(cmd, "Pipeline step")

    def start_prediction(self):
        cmd = (
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
        self._ssh(cmd, check=False)

    # ---- Tunnel ----

    def ensure_iap(self):
        """Enable IAP API and add firewall rule for IAP tunnel access"""
        log("Enabling IAP API...")
        subprocess.run(
            ["gcloud", "services", "enable", "iap.googleapis.com",
             "--project", self.project],
            capture_output=True, timeout=60)

        rules = subprocess.run(
            ["gcloud", "compute", "firewall-rules", "list",
             "--filter=name=allow-iap", "--project", self.project,
             "--format=value(name)"],
            capture_output=True, text=True, timeout=10)
        if "allow-iap" not in rules.stdout:
            log("Creating IAP firewall rule...")
            subprocess.run([
                "gcloud", "compute", "firewall-rules", "create", "allow-iap",
                "--direction=INGRESS", "--priority=1000",
                "--network=default", "--action=ALLOW",
                "--rules=tcp:5001,tcp:5002",
                "--source-ranges=35.235.240.0/20",
                "--project", self.project,
            ], capture_output=True, check=True, timeout=30)
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
        cmd = [
            "gcloud", "container", "--project", self.project,
            "clusters", "create", "ibdn-cluster",
            "--zone", self.zone, "--num-nodes=3",
            "--machine-type=e2-small", "--disk-size=30",
        ]
        subprocess.run(cmd, check=True)

    def deploy_k8s(self):
        k8s_dir = os.path.join(os.path.dirname(__file__), "k8s")
        if not os.path.isdir(k8s_dir):
            log("No k8s/ directory found.")
            return
        subprocess.run(["kubectl", "apply", "-f", k8s_dir], check=True)

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
                self.deploy_compose()  # pull + build + up
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
