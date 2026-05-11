import os, time, subprocess

class GCPOrchestrator:
    def __init__(self, mode="gcloud", db="cassandra"):
        self.mode = mode
        self.db = db
        self.project = os.getenv("GCP_PROJECT") or self._detect_project()
        self.zone = os.getenv("GCP_ZONE", "europe-west1-b")
        self.instance = os.getenv("GCP_INSTANCE", "bigdata-vm")
        self.user = os.getenv("GCP_USER", "ubuntu")
        self.repo = os.getenv("REMOTE_REPO", os.getcwd())
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
        cmd = self._base + [
            "ssh", f"{self.user}@{self.instance}",
            "--zone", self.zone, "--command", command, "--quiet",
        ]
        return subprocess.run(cmd, capture_output=True, text=True, **kwargs)

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
            "--boot-disk-size=30",
            "--metadata-from-file", f"user-data={cloud_init}",
        ]
        subprocess.run(cmd, check=True)
        log("VM created. Waiting for SSH...")

    def start_vm(self):
        self._gcloud("instances", "start", self.instance, "--zone", self.zone, check=True)

    def stop_vm(self):
        self._gcloud("instances", "stop", self.instance, "--zone", self.zone, check=True)

    def wait_for_vm(self, timeout=180):
        for _ in range(timeout):
            r = self._ssh("echo ready")
            if r.returncode == 0:
                return
            time.sleep(2)
        raise TimeoutError("VM not ready after %ds" % timeout)

    def get_external_ip(self):
        r = self._gcloud(
            "instances", "describe", self.instance, "--zone", self.zone,
            "--format=value(networkInterfaces[0].accessConfigs[0].natIP)", check=True)
        return r.stdout.strip()

    # ---- Deploy ----

    def deploy_compose(self):
        commands = [
            f"cd {self.repo} && docker compose down 2>/dev/null; true",
            f"cd {self.repo} && docker compose up -d --build",
        ]
        for cmd in commands:
            self._ssh(cmd, check=True)

    def run_pipeline(self):
        env = f"DB_MODE={self.db}"
        commands = [
            f"cd {self.repo} && {env} python3 scripts/create_bucket.py",
            f"cd {self.repo} && {env} python3 scripts/download_data.py",
            f"cd {self.repo} && {env} python3 scripts/import_distances.py",
        ]
        for cmd in commands:
            self._ssh(cmd, check=True)

    def register_original_model(self):
        cmd = (
            f"cd {self.repo} && docker exec spark spark-submit --master spark://spark:7077 "
            f"--conf spark.hadoop.fs.s3a.access.key=admin --conf spark.hadoop.fs.s3a.secret.key=password "
            f"scripts/register_original.py"
        )
        self._ssh(cmd, check=False)

    def start_prediction(self):
        cmd = (
            f"cd {self.repo} && docker exec -d spark spark-submit --master spark://spark:7077 "
            f"--deploy-mode cluster --conf spark.cores.max=2 "
            f"--conf spark.hadoop.fs.s3a.access.key=admin --conf spark.hadoop.fs.s3a.secret.key=password "
            f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            f"--conf spark.hadoop.fs.s3a.path.style.access=true "
            f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            f"--class es.upm.dit.ging.predictor.MakePrediction "
            f"/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
        )
        self._ssh(cmd, check=False)

    # ---- Tunnel ----

    def tunnel(self):
        ip = self.get_external_ip()
        log(f"Tunnel: localhost:5001 -> {ip}:5001")
        cmd = self._base + [
            "ssh", f"{self.user}@{self.instance}",
            "--zone", self.zone,
            "--", "-L", "5001:localhost:5001",
            "-L", "5002:localhost:5002", "-N",
        ]
        subprocess.run(cmd)

    @staticmethod
    def suggest_tunnel():
        print()
        print("=" * 60)
        print("  Deployment complete!")
        print("  Use `gcloud compute ssh ... -- -L 5001:localhost:5001 -L 5002:localhost:5002 -N` to access")
        print("=" * 60)

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
                self.deploy_compose()
                self.run_pipeline()
                self.register_original_model()
                self.start_prediction()
                self.suggest_tunnel()
                log("Opening tunnel (Ctrl+C to stop and shut down VM)...")
                self.tunnel()
            except KeyboardInterrupt:
                log("Interrupted.")
            finally:
                self.stop_vm()

        elif self.mode == "gke":
            self.create_gke_cluster()
            self.deploy_k8s()
            self.suggest_tunnel()
