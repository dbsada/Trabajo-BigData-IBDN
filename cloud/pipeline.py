import os
import sys
import time
from utils.shell import sh
from utils.config import load_dotenv
from cloud.config import DeployConfig
from cloud import docker_ops, spark_ops, kafka_ops

load_dotenv()


@sh
def run_script(cfg, script_name):
    return f"{cfg.venv_python} {os.path.join(cfg.project_home, 'scripts', script_name)}"


def send_progress(step, status, message=""):
    flk_port = os.getenv('FLASK_PORT', '5001')
    vm_ip = 'localhost'
    try:
        result = docker_ops._get_vm_ip()
        vm_ip = result.stdout.strip().split()[0]
    except Exception:
        pass
    try:
        import requests
        requests.post(
            f"http://{vm_ip}:{flk_port}/api/pipeline/progress",
            json={"step": step, "status": status, "message": message},
            timeout=2,
        )
    except Exception:
        pass


def run_pipeline(cfg):
    from rich.console import Console

    console = Console()

    os.environ['SKIP_AUTO_START_PREDICTION'] = '1'
    os.environ['DB_MODE'] = cfg.db_mode

    # Clear stale pipeline state
    state_file = '/tmp/pipeline_state.json'
    if os.path.exists(state_file):
        os.remove(state_file)

    docker_ops.compose_down(cfg.project_home)
    time.sleep(3)

    console.print("[dim]Starting all services...[/dim]")
    docker_ops.compose_up(cfg.project_home, cfg.db_mode)

    console.print("[dim]Waiting for Flask...[/dim]")
    docker_ops.wait_for_flask(cfg)

    vm_ip = 'localhost'
    try:
        result = docker_ops._get_vm_ip()
        vm_ip = result.stdout.strip().split()[0]
    except Exception:
        pass

    flk_port = os.getenv('FLASK_PORT', '5001')
    console.print(f"[dim]Flask available at http://{vm_ip}:{flk_port}[/dim]")

    send_progress("core_services", "running", "Starting services...")

    console.print("[dim]Waiting for Spark, MinIO, MLflow...[/dim]")
    send_progress("infra_services", "running", "Starting Spark, MinIO, MLflow...")
    docker_ops.wait_for_infra(cfg)
    send_progress("infra_services", "done", "All services ready")
    send_progress("core_services", "done", "Core services ready")

    console.print("[dim]Running pipeline scripts...[/dim]")
    send_progress("buckets", "running", "Creating MinIO buckets...")
    r = run_script(cfg, 'create_bucket.py')
    if r.returncode != 0:
        send_progress("buckets", "failed", "Bucket creation failed")
        sys.exit(1)
    send_progress("buckets", "done", "Buckets created")

    send_progress("download", "running", "Downloading flight data...")
    r = run_script(cfg, 'download_data.py')
    if r.returncode != 0:
        send_progress("download", "failed", "Download failed")
        sys.exit(1)
    send_progress("download", "done", "Data downloaded")

    docker_ops.upload_data_to_minio(cfg)
    send_progress("upload", "done", "Data uploaded to MinIO")

    send_progress("import_distances", "running", "Importing distance data...")
    r = run_script(cfg, 'import_distances.py')
    if r.returncode != 0:
        send_progress("import_distances", "failed", "Import failed")
        sys.exit(1)
    send_progress("import_distances", "done", "Distances imported")

    kafka_ops.create_all_topics()
    send_progress("topics", "done", "Kafka topics created")

    send_progress("prediction", "running", "Starting prediction engine...")
    result = spark_ops.predict_delay(cfg)
    if result is None:
        send_progress("prediction", "failed", "Prediction job failed")
        sys.exit(1)
    send_progress("prediction", "done", "Prediction engine started")

    send_progress("done", "done", "Ready")

    console.print()
    console.print(f"[bold green]Cluster ready[/bold green] at [link=http://{vm_ip}:{flk_port}]http://{vm_ip}:{flk_port}[/link]")
    console.print("[dim]Press Ctrl+C to shutdown[/dim]")

    shutting_down = False
    def _shutdown(signum=None, frame=None):
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        send_progress("done", "failed", "Shutdown by user")
        console.print()
        try:
            docker_ops.compose_down(cfg.project_home)
        except Exception:
            pass
        console.print("[dim]Goodbye![/dim]")
        import signal
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        raise SystemExit(0)

    import signal
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown()
