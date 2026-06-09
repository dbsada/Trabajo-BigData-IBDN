import sys
import subprocess
import os
from config import DeployConfig
from pipeline import local, gke


def _check_docker():
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        if r.returncode != 0:
            sys.exit("Docker is not running. Start Docker Desktop and try again.")
    except FileNotFoundError:
        sys.exit("Docker is not installed. Install Docker Desktop first.")
    except subprocess.TimeoutExpired:
        sys.exit("Docker is not responding. Restart Docker Desktop and try again.")


class Orchestrator:
    def __init__(self, mode: str, cfg: DeployConfig):
        self.mode = mode
        self.cfg = cfg

    def run(self):
        _check_docker()
        if self.mode == 'docker':
            local.run_pipeline(self.cfg)
        elif self.mode == 'gke':
            gke.run_pipeline_gke(self.cfg)
        else:
            raise ValueError(f"Unknown mode: {self.mode}")