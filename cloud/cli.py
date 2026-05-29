"""Cluster Orchestrator CLI — Flight Delay Prediction"""

import typer
from rich.console import Console
from rich.text import Text

from utils.config import load_dotenv
from cloud.config import DeployConfig
from cloud.orchestrator import Orchestrator

load_dotenv()

app = typer.Typer(
    name="f-pred",
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
def gcloud_docker(db: str = "cassandra"):
    """Deploy to GCloud VM with Docker Compose."""
    db_mode = "mongo" if "mongo" in db.lower() else "cassandra"
    db_label = "MongoDB" if db_mode == "mongo" else "Cassandra"
    console.print(Text.assemble(
        ("⚙️ ", "bold yellow"), ("Docker (GCloud)", "bold white"),
        (" + ", "dim white"), (db_label, "bold cyan"),
    ))
    cfg = DeployConfig.from_env(db_mode=db_mode)
    Orchestrator(mode="gcloud", cfg=cfg).run()


@app.command()
def gcloud_kubernetes():
    """Deploy to GKE cluster."""
    console.print(Text.assemble(
        ("⚙️ ", "bold yellow"), ("Kubernetes (GKE)", "bold white"),
    ))
    cfg = DeployConfig.from_env()
    Orchestrator(mode="gke", cfg=cfg).run()


def main():
    app()


if __name__ == "__main__":
    main()
