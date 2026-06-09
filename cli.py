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
def docker():
    """Deploy locally with Docker Compose."""
    console.print(Text.assemble(
        ("⚙️ ", "bold yellow"), ("Docker", "bold white"),
        (" + ", "dim white"), ("Cassandra", "bold cyan"),
    ))
    cfg = DeployConfig.from_env(db_mode="cassandra")
    Orchestrator(mode="docker", cfg=cfg).run()

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
def push():
    """Push locally trained models + MLflow DB to GKE MinIO."""
    _push_to_gke()

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