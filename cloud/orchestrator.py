from cloud.config import DeployConfig
from cloud import pipeline, pipeline_gcloud


class Orchestrator:
    def __init__(self, mode: str, cfg: DeployConfig):
        self.mode = mode
        self.cfg = cfg

    def run(self):
        if self.mode == 'docker':
            pipeline.run_pipeline(self.cfg)
        elif self.mode == 'gcloud':
            pipeline_gcloud.run_pipeline_gcloud(self.cfg)
        elif self.mode == 'gke':
            pipeline_gcloud.run_pipeline_gke(self.cfg)
        else:
            raise ValueError(f"Unknown mode: {self.mode}")
