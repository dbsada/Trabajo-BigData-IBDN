"""Cluster Orchestrator CLI — Flight Delay Prediction"""

import typer
from rich.console import Console
from rich.text import Text

from utils.config import load_dotenv
from config import DeployConfig
from orchestrator import Orchestrator

load_dotenv()

app = typer.Typer(
    name="predict",
    help="Flight Delay Prediction — Cluster Orchestrator",
    add_completion=False,
)

console = Console()

@app.command()
def docker(db: str = "cassandra"):
    """Deploy locally with Docker Compose."""
    db_mode = "mongo" if "mongo" in db.lower() else "cassandra"
    db_label = "MongoDB" if db_mode == "mongo" else "Cassandra"
    console.print(Text.assemble(
        ("⚙️ ", "bold yellow"), ("Docker", "bold white"),
        (" + ", "dim white"), (db_label, "bold cyan"),
    ))
    cfg = DeployConfig.from_env(db_mode=db_mode)
    Orchestrator(mode="docker", cfg=cfg).run()

@app.command()
def gcloud(db: str = "cassandra"):
    """Deploy to GCloud VM with Docker Compose."""
    db_mode = "mongo" if "mongo" in db.lower() else "cassandra"
    db_label = "MongoDB" if db_mode == "mongo" else "Cassandra"
    console.print(Text.assemble(
        ("⚙️ ", "bold yellow"), ("GCloud VM", "bold white"),
        (" + ", "dim white"), (db_label, "bold cyan"),
    ))
    cfg = DeployConfig.from_env(db_mode=db_mode)
    Orchestrator(mode="gcloud", cfg=cfg).run()

@app.command()
def gke():
    """Deploy to GKE cluster."""
    console.print(Text.assemble(
        ("⚙️ ", "bold yellow"), ("GKE (Kubernetes)", "bold white"),
    ))
    cfg = DeployConfig.from_env()
    Orchestrator(mode="gke", cfg=cfg).run()


@app.command()
def gke_down():
    """Scale GKE cluster to 0 nodes (stop paying for workers)."""
    from pipeline import gcp_ops
    gcp = gcp_ops.GCPConfig()
    console.print("[dim]Scaling GKE cluster to 0 nodes...[/dim]")
    gcp_ops.scale_gke_cluster(gcp, 0)
    console.print("[bold green]GKE cluster scaled to 0 nodes. Workers stopped.[/bold green]")
    console.print("[dim]Control plane is free. Run predict gke to scale up and deploy.[/dim]")


models_app = typer.Typer(help="Model operations (push/pull between environments)")
app.add_typer(models_app, name="models")


@models_app.command()
def push(gke: bool = typer.Option(False, "--gke", help="Push to GKE cluster instead of GCloud VM")):
    """Push locally trained models + MLflow DB to GCloud VM or GKE cluster."""
    import subprocess, tempfile, os, time
    from pipeline import gcp_ops

    if gke:
        _push_to_gke()
        return

    gcp = gcp_ops.GCPConfig()
    console.print("[bold]Step 1/4:[/bold] Checking VM status...")
    vm_status = subprocess.run(
        f"gcloud compute instances describe {gcp.instance} --zone {gcp.zone} "
        f"--project {gcp.project} --format=value(status)",
        shell=True, capture_output=True, text=True
    ).stdout.strip()
    if vm_status != "RUNNING":
        console.print("[yellow]VM is not running. Starting it...[/yellow]")
        subprocess.run(
            f"gcloud compute instances start {gcp.instance} --zone {gcp.zone} "
            f"--project {gcp.project}", shell=True, check=True
        )
        time.sleep(30)

    console.print("[bold]Step 2/4:[/bold] Exporting models from local MinIO...")
    minio_volume = "practica_creativa_minio_data"
    check = subprocess.run(
        f"docker volume inspect {minio_volume}", shell=True, capture_output=True
    )
    if check.returncode != 0:
        console.print("[red]Local MinIO volume not found. Run predict docker first to create it.[/red]")
        return

    with tempfile.TemporaryDirectory() as tmp:
        tar_path = os.path.join(tmp, "models.tar.gz")
        r = subprocess.run(
            f"docker run --rm -v {minio_volume}:/data alpine tar -czf /tmp/out.tar.gz -C /data/lakehouse/models . 2>/dev/null; "
            f"docker cp $(docker create --name tmp_export alpine):/tmp/out.tar.gz /dev/stdout 2>/dev/null; "
            f"docker rm tmp_export 2>/dev/null; true",
            shell=True, capture_output=True
        )
        subprocess.run(
            f"docker run --rm -v {minio_volume}:/data -v {tmp}:/out alpine sh -c 'tar -czf /out/models.tar.gz -C /data/lakehouse/models . 2>/dev/null; true'",
            shell=True
        )
        if not os.path.exists(tar_path) or os.path.getsize(tar_path) < 100:
            console.print("[red]No trained models found locally. Train one first via the Models tab.[/red]")
            return
        console.print(f"[dim]  Exported {os.path.getsize(tar_path)/1024:.0f} KB[/dim]")

        console.print("[bold]Step 3/4:[/bold] Uploading models to VM...")
        remote_tmp = f"{gcp.repo}/models_upload.tar.gz"
        subprocess.run(
            f"gcloud compute scp {tar_path} {gcp.user}@{gcp.instance}:{remote_tmp} "
            f"--zone {gcp.zone} --project {gcp.project} --quiet",
            shell=True, check=True
        )

        console.print("[bold]Step 4/4:[/bold] Importing into VM MinIO...")
        cmds = [
            f"docker cp {gcp.repo}/models_upload.tar.gz minio:/tmp/models_upload.tar.gz",
            "docker exec minio sh -c 'cd /data/lakehouse/models && tar -xzf /tmp/models_upload.tar.gz 2>/dev/null; true'",
            "docker exec minio rm -f /tmp/models_upload.tar.gz",
            f"rm -f {gcp.repo}/models_upload.tar.gz",
        ]
        cmd_str = " && ".join(cmds)
        r = subprocess.run(
            f"gcloud compute ssh {gcp.user}@{gcp.instance} --zone {gcp.zone} "
            f"--project {gcp.project} --quiet --command {repr(cmd_str)}",
            shell=True
        )
        if r.returncode == 0:
            console.print("[bold green]Models pushed successfully![/bold green]")

            mlflow_db = "data/mlflow/mlflow.db"
            if os.path.exists(mlflow_db) and os.path.getsize(mlflow_db) > 1000:
                console.print("[dim]  Copying MLflow database to VM...[/dim]")
                remote_db = f"{gcp.repo}/mlflow.db"
                subprocess.run(
                    f"gcloud compute scp {mlflow_db} {gcp.user}@{gcp.instance}:{remote_db} "
                    f"--zone {gcp.zone} --project {gcp.project} --quiet",
                    shell=True, check=True
                )
                db_cmds = [
                    "docker cp mlflow:/data/mlflow/mlflow.db /tmp/mlflow_backup.db 2>/dev/null; true",
                    f"docker cp {remote_db} mlflow:/data/mlflow/mlflow.db",
                    f"cd {gcp.repo} && docker compose --profile db_cassandra restart mlflow",
                    f"rm -f {remote_db}",
                ]
                subprocess.run(
                    f"gcloud compute ssh {gcp.user}@{gcp.instance} --zone {gcp.zone} "
                    f"--project {gcp.project} --quiet --command {repr(' && '.join(db_cmds))}",
                    shell=True
                )

            console.print("[dim]  Restarting prediction job...[/dim]")
            subprocess.run(
                f"gcloud compute ssh {gcp.user}@{gcp.instance} --zone {gcp.zone} "
                f"--project {gcp.project} --quiet "
                f"--command 'cd {gcp.repo} && docker exec spark-manager spark-submit "
                f"--master spark://spark-manager:7077 --deploy-mode cluster --conf spark.cores.max=2 "
                f"--conf spark.hadoop.fs.s3a.access.key=admin "
                f"--conf spark.hadoop.fs.s3a.secret.key=password "
                f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
                f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
                f"--conf spark.hadoop.fs.s3a.path.style.access=true "
                f"--class es.upm.dit.ging.predictor.MakePrediction "
                f"/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar'",
                shell=True, capture_output=True
            )
            console.print("[dim]  Done. Models should appear in the Models tab now.[/dim]")
        else:
            console.print("[red]Failed to import models on VM.[/red]")

def _push_to_gke():
    """Push models + MLflow DB from local Docker to GKE."""
    import subprocess, tempfile, os, time, boto3
    from botocore.config import Config

    namespace = "ibdn"
    minio_volume = "practica_creativa_minio_data"
    project_home = os.path.dirname(os.path.abspath(__file__))

    console.print("[bold]Step 1/5:[/bold] Exporting models from local MinIO...")
    check = subprocess.run(
        f"docker volume inspect {minio_volume}", shell=True, capture_output=True
    )
    if check.returncode != 0:
        console.print("[red]Local MinIO volume not found. Run predict docker first.[/red]")
        return

    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            f"docker run --rm -v {minio_volume}:/data -v {tmp}:/out alpine sh -c '"
            f"tar -czf /out/models.tar.gz -C /data/lakehouse/models . 2>/dev/null; true'",
            shell=True
        )

        console.print("[bold]Step 2/5:[/bold] Checking kubectl connection...")
        r = subprocess.run(
            "kubectl config current-context 2>/dev/null", shell=True, capture_output=True, text=True
        )
        if not r.stdout.strip():
            console.print("[red]No kubectl context found. Run predict gke first.[/red]")
            return

        console.print("[bold]Step 3/5:[/bold] Starting MinIO port-forward...")
        pf = subprocess.Popen(
            f"kubectl port-forward -n {namespace} deploy/minio 9001:9000",
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        time.sleep(4)

        console.print("[bold]Step 4/5:[/bold] Uploading models to GKE MinIO...")
        models_tar = os.path.join(tmp, "models.tar.gz")
        if os.path.exists(models_tar) and os.path.getsize(models_tar) > 100:
            s3 = boto3.client("s3",
                endpoint_url="http://localhost:9001",
                aws_access_key_id="admin",
                aws_secret_access_key="password",
                config=Config(signature_version="s3v4"))
            try:
                s3.upload_file(models_tar, "lakehouse", "models.tar.gz")
                console.print("[dim]  models.tar.gz uploaded.[/dim]")
                # Extract on GKE via kubectl exec
                subprocess.run(
                    f"kubectl exec -n {namespace} deploy/minio -- sh -c '"
                    f"cd /data/lakehouse/models && tar -xzf /data/lakehouse/models.tar.gz 2>/dev/null; true'",
                    shell=True
                )
                console.print("[dim]  Models extracted on GKE.[/dim]")
            except Exception as e:
                console.print(f"[yellow]  Upload issue: {e}[/yellow]")
        else:
            console.print("[yellow]  No local models found.[/yellow]")

        pf.terminate()

    console.print("[bold]Step 5/5:[/bold] Uploading MLflow database...")
    local_db = os.path.join(project_home, "data", "mlflow", "mlflow.db")
    if os.path.exists(local_db):
        r = subprocess.run(
            f"kubectl exec -n {namespace} -i deploy/mlflow -- sh -c 'cat > /data/mlflow/mlflow.db' < {local_db}",
            shell=True, capture_output=True, text=True
        )
        if r.returncode == 0:
            console.print("[dim]  MLflow database synced.[/dim]")
        else:
            console.print(f"[yellow]  MLflow DB upload issue: {r.stderr[:100]}[/yellow]")
    else:
        console.print("[yellow]  No local MLflow DB at data/mlflow/mlflow.db[/yellow]")

    console.print("[bold]Restarting MLflow on GKE...[/bold]")
    subprocess.run(
        f"kubectl rollout restart deployment/mlflow -n {namespace}", shell=True
    )
    time.sleep(10)
    console.print("[bold green]Push complete![/bold green]")
    console.print("[dim]Refresh the Models tab and activate a model.[/dim]")

def main():
    app()

if __name__ == "__main__":
    main()