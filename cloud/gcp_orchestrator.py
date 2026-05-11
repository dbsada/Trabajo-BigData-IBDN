import os, time, subprocess, logging, socket

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GCPOrchestrator:
    def __init__(self, mode="gcloud", db="cassandra"):
        self.mode = mode
        self.db = db
        self.project = os.getenv("GCP_PROJECT", self._detect_project())
        self.zone = os.getenv("GCP_ZONE", "europe-west1-b")
        self.instance = os.getenv("GCP_INSTANCE", "bigdata-vm")
        self.user = os.getenv("GCP_USER", "ubuntu")
        self.repo = os.getenv("REMOTE_REPO", os.getcwd())

    def _detect_project(self):
        try:
            r = subprocess.run(["gcloud", "config", "get-value", "project"], capture_output=True, text=True, check=True)
            return r.stdout.strip()
        except Exception:
            return "my-project"

    def _gcloud(self, *args, **kwargs):
        cmd = ["gcloud", "compute"] + list(args)
        logging.info(f"Running: {' '.join(cmd)}")
        return subprocess.run(cmd, capture_output=True, text=True, **kwargs)

    def _ssh(self, command, **kwargs):
        cmd = [
            "gcloud", "compute", "ssh",
            f"{self.user}@{self.instance}",
            "--zone", self.zone,
            "--command", command,
            "--quiet",
        ]
        logging.info(f"SSH: {command[:80]}...")
        return subprocess.run(cmd, capture_output=True, text=True, **kwargs)

    def _scp(self, source, dest, **kwargs):
        cmd = [
            "gcloud", "compute", "scp",
            "--zone", self.zone, "--quiet",
            source, f"{self.user}@{self.instance}:{dest}",
        ]
        logging.info(f"SCP: {source} -> {dest}")
        return subprocess.run(cmd, capture_output=True, text=True, **kwargs)

    # ---- VM Control ----

    def start_vm(self):
        logging.info("Starting VM...")
        self._gcloud("instances", "start", self.instance, "--zone", self.zone, check=True)

    def stop_vm(self):
        logging.info("Stopping VM...")
        self._gcloud("instances", "stop", self.instance, "--zone", self.zone, check=True)

    def wait_for_vm(self, timeout=120):
        logging.info("Waiting for SSH...")
        for _ in range(timeout):
            r = self._ssh("echo ready")
            if r.returncode == 0:
                logging.info("VM ready")
                return
            time.sleep(2)
        raise TimeoutError("VM not ready after %ds" % timeout)

    def get_external_ip(self):
        r = self._gcloud(
            "instances", "describe", self.instance,
            "--zone", self.zone,
            "--format=value(networkInterfaces[0].accessConfigs[0].natIP)",
            check=True,
        )
        return r.stdout.strip()

    # ---- Deploy ----

    def deploy_compose(self):
        logging.info("Deploying stack via Docker Compose...")
        commands = [
            f"cd {self.repo} && docker compose down 2>/dev/null; true",
            f"cd {self.repo} && docker compose up -d --build",
        ]
        for cmd in commands:
            self._ssh(cmd, check=True)

    def run_pipeline(self):
        logging.info("Running pipeline...")
        env = f"DB_MODE={self.db}"
        commands = [
            f"cd {self.repo} && {env} python3 scripts/create_bucket.py",
            f"cd {self.repo} && {env} python3 scripts/download_data.py",
            f"cd {self.repo} && {env} python3 scripts/import_distances.py",
        ]
        for cmd in commands:
            self._ssh(cmd, check=True)

    def register_original_model(self):
        logging.info("Registering original model in MLflow...")
        env = "MLFLOW_TRACKING_URI=http://mlflow:5000"
        cmd = f"cd {self.repo} && docker exec spark spark-submit --master spark://spark:7077 --conf spark.hadoop.fs.s3a.access.key=admin --conf spark.hadoop.fs.s3a.secret.key=password scripts/register_original.py"
        self._ssh(cmd, check=False)

    def start_prediction(self):
        logging.info("Starting prediction job...")
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
        logging.info(f"Opening tunnel to {ip}:5001 -> localhost:5001")
        logging.info(f"Open http://localhost:5001 in your browser")
        subprocess.run([
            "gcloud", "compute", "ssh",
            f"{self.user}@{self.instance}",
            "--zone", self.zone,
            "--", "-L", "5001:localhost:5001",
            "-L", "5002:localhost:5002",
            "-N",
        ])

    def suggest_tunnel(self):
        ip = self.get_external_ip()
        print()
        print("=" * 60)
        print(f"  VM external IP: {ip}")
        print(f"  Flask:      http://{ip}:5001")
        print(f"  MLflow:     http://{ip}:5002")
        print()
        print("  Or use tunnel:")
        print(f"    gcloud compute ssh {self.user}@{self.instance} --zone {self.zone} -- -L 5001:localhost:5001 -L 5002:localhost:5002 -N")
        print("=" * 60)

    # ---- K8S (GKE) ----

    def create_gke_cluster(self):
        logging.info("Creating GKE cluster...")
        cmd = [
            "gcloud", "container", "clusters", "create", "ibdn-cluster",
            "--zone", self.zone,
            "--num-nodes=3",
            "--machine-type=e2-small",
            "--disk-size=30",
        ]
        subprocess.run(cmd, check=True)

    def deploy_k8s(self):
        logging.info("Deploying to GKE...")
        k8s_dir = os.path.join(os.path.dirname(__file__), "k8s")
        if not os.path.isdir(k8s_dir):
            logging.warning("No k8s/ directory found. Create manifests first.")
            return
        subprocess.run(["kubectl", "apply", "-f", k8s_dir], check=True)

    # ---- Full Flow ----

    def run(self):
        if self.mode == "gcloud":
            try:
                self.start_vm()
                self.wait_for_vm()
                self.deploy_compose()
                self.run_pipeline()
                self.register_original_model()
                self.start_prediction()
                self.suggest_tunnel()
                logging.info("Deployment complete. Press Ctrl+C to stop the tunnel.")
                self.tunnel()
            except KeyboardInterrupt:
                logging.info("Interrupted.")
            finally:
                self.stop_vm()

        elif self.mode == "gke":
            self.create_gke_cluster()
            self.deploy_k8s()
            self.suggest_tunnel()
