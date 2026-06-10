import os
import time
import subprocess
import logging
import tempfile
from utils.shell import sh

CLOUD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloud")

class GCPConfig:
    def __init__(self):
        self.project = os.getenv("GCP_PROJECT") or self._detect_project()
        self.zone = os.getenv("GCP_ZONE", "europe-west1-b")

    def _detect_project(self):
        try:
            r = subprocess.run(
                ["gcloud", "config", "get-value", "project"],
                capture_output=True, text=True, check=True)
            return r.stdout.strip()
        except Exception:
            raise RuntimeError("GCP_PROJECT not set. Set it in .env")

# ---- GKE ----
@sh
def _gcloud_create_gke_cluster(gcp):
    machine_type = os.getenv('GKE_MACHINE_TYPE', 'e2-standard-4')
    num_nodes = os.getenv('GKE_NUM_NODES', '4')
    cluster_name = os.getenv('GKE_CLUSTER_NAME', 'ibdn-cluster')
    disk_size = os.getenv('GKE_DISK_SIZE', '30')
    return (
        f"gcloud container --project {gcp.project} "
        f"clusters create {cluster_name} "
        f"--zone {gcp.zone} --num-nodes={num_nodes} "
        f"--machine-type={machine_type} --disk-size={disk_size}"
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
        f"gcloud container clusters list --filter='name={cluster_name} AND status=RUNNING' --format='value(name)'"
    )

def wait_for_gke_cluster(gcp):
    import time
    logging.info("Waiting for GKE cluster to be ready...")
    for _ in range(60):
        r = _wait_for_gke_cluster(gcp)
        if r.stdout.strip():
            logging.info("GKE cluster is ready")
            return
        time.sleep(10)
    raise RuntimeError("GKE cluster did not become ready within 10 minutes")

def build_and_push_images(cfg):
    registry = os.getenv('ARTIFACT_REGISTRY', f"europe-west1-docker.pkg.dev/{os.getenv('GCP_PROJECT', '')}/ibdn")
    tag = os.getenv('IMAGE_TAG', 'latest')
    project = os.getenv('GCP_PROJECT', '')

    logging.info(f"Checking if images exist in {registry}...")
    r = subprocess.run(
        f"gcloud artifacts docker images list {registry} --include-tags --format='value(tags)' 2>/dev/null | grep -q '^{tag}$'",
        shell=True, capture_output=True, text=True
    )
    if r.returncode == 0:
        logging.info(f"Images with tag '{tag}' already exist, skipping build")
        return

    logging.info(f"Building and pushing images to {registry} via Cloud Build (single batch)")

    logging.info(f"Building and pushing images to {registry} via Cloud Build (single batch)")

    images = [
        ("spark-base", "docker/dockerfile.spark-base", None),
        ("spark", "docker/dockerfile.spark", f"BASE_IMAGE={registry}/spark-base:4.1.1"),
        ("flask", "docker/dockerfile.python", None),
        ("airflow", "docker/dockerfile.airflow", None),
    ]

    steps = "steps:\n"
    for name, dockerfile, build_arg in images:
        tags = [f"{registry}/{name}:{tag}"]
        if name == "spark-base":
            tags.append(f"{registry}/{name}:4.1.1")
        args_list = ["build"]
        for t in tags:
            args_list.extend(["-t", t])
        args_list.extend(["-f", dockerfile])
        if build_arg:
            args_list.extend(["--build-arg", build_arg])
        args_list.append(".")
        args_yaml = "[" + ", ".join(f"'{a}'" for a in args_list) + "]"
        steps += f"""
- name: 'gcr.io/cloud-builders/docker'
  args: {args_yaml}
"""

    images_list = [f"- '{registry}/{name}:{tag}'" for name, _, _ in images]
    if "spark-base:4.1.1" not in str(images_list):
        images_list.append(f"- '{registry}/spark-base:4.1.1'")
    steps += "\nimages:\n" + "\n".join(images_list) + "\n"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(steps)
        config_path = f.name

    subprocess.run(
        f"gcloud builds submit --config={config_path} --machine-type=e2-highcpu-8 --timeout=7200 .",
        shell=True, check=True, cwd=cfg.project_home
    )
    os.unlink(config_path)

    logging.info("Images built and pushed")


def substitute_image_refs(cfg):
    project = os.getenv('GCP_PROJECT', '')
    k8s_dir = os.path.join(CLOUD_DIR, "k8s")
    import tempfile as _tf
    tmp_manifests = _tf.mkdtemp()
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
    return f"kubectl apply -f {manifest_dir} --validate=false --request-timeout=60s"


def deploy_k8s(gcp, cfg=None):
    k8s_dir = os.path.join(CLOUD_DIR, "k8s")
    if not os.path.isdir(k8s_dir):
        logging.info("No k8s/ directory found.")
        return
    if subprocess.run("which gke-gcloud-auth-plugin", shell=True, capture_output=True).returncode != 0:
        logging.info("Installing gke-gcloud-auth-plugin...")
        subprocess.run("gcloud components install gke-gcloud-auth-plugin --quiet",
                      shell=True, check=True)

    # Wait for API server to be ready
    for _ in range(30):
        r = subprocess.run("kubectl cluster-info 2>/dev/null", shell=True, capture_output=True)
        if r.returncode == 0:
            break
        logging.info("Waiting for Kubernetes API server...")
        time.sleep(10)

    # Ensure namespace exists before creating ConfigMaps
    subprocess.run("kubectl create namespace ibdn --dry-run=client -o yaml | kubectl apply -f -", shell=True, check=True)

    # Create required ConfigMaps from source files
    project_home = cfg.project_home if cfg else os.path.dirname(CLOUD_DIR)
    configmaps = {
        "flask-code-patch": {"predict_flask.py": os.path.join(project_home, "scripts", "web", "predict_flask.py"),
                             "predict_utils.py": os.path.join(project_home, "scripts", "web", "predict_utils.py")},
        "airflow-dags": {"train_flight_delay_model.py": os.path.join(project_home, "dags", "train_flight_delay_model.py")},
    }
    for cm_name, files in configmaps.items():
        cm_cmd = f"kubectl create configmap -n ibdn {cm_name} --dry-run=client -o yaml"
        for key, path in files.items():
            if os.path.exists(path):
                cm_cmd += f" --from-file={key}={path}"
        cm_cmd += " | kubectl apply -f -"
        subprocess.run(cm_cmd, shell=True, check=True)

    manifest_dir = substitute_image_refs(gcp)
    for attempt in range(5):
        r = _kubectl_apply_k8s(manifest_dir)
        if r.returncode == 0:
            break
        logging.warning(f"kubectl apply failed (attempt {attempt+1}/5), retrying...")
        time.sleep(10)
    else:
        raise RuntimeError("kubectl apply failed after 5 attempts")
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
    console.print("[dim]Press Ctrl+C to stop port-forwards[/dim]")
    try:
        import signal as _sig
        _sig.pause()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        console.print("[dim]Stopping port forwards...[/dim]")
    finally:
        for p in procs:
            p.terminate()
