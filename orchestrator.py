import sys
import subprocess
import os
from config import DeployConfig
from pipeline import local, gcloud


def _check_docker():
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        if r.returncode != 0:
            sys.exit("Docker is not running. Start Docker Desktop and try again.")
    except FileNotFoundError:
        sys.exit("Docker is not installed. Install Docker Desktop first.")
    except subprocess.TimeoutExpired:
        sys.exit("Docker is not responding. Restart Docker Desktop and try again.")


def _stop_local_if_running(project_home):
    import docker as dk
    try:
        client = dk.from_env()
        flask = client.containers.get("flask")
        if flask.status == "running":
            print("🛑 Stopping local containers (ports conflict with cloud)...")
            subprocess.run(
                f"cd \"{project_home}\" && docker compose --profile db_mongo --profile db_cassandra down 2>/dev/null",
                shell=True, capture_output=True
            )
            print("   Local environment stopped.")
    except Exception:
        pass


class Orchestrator:
    def __init__(self, mode: str, cfg: DeployConfig):
        self.mode = mode
        self.cfg = cfg

    def run(self):
        _check_docker()
        if self.mode in ('gcloud', 'gke'):
            _stop_local_if_running(self.cfg.project_home)
        if self.mode == 'docker':
            local.run_pipeline(self.cfg)
        elif self.mode == 'gcloud':
            gcloud.run_pipeline_gcloud(self.cfg)
        elif self.mode == 'gke':
            gcloud.run_pipeline_gke(self.cfg)
        else:
            raise ValueError(f"Unknown mode: {self.mode}")