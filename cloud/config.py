import os
from dataclasses import dataclass, field


@dataclass
class DeployConfig:
    project_home: str = field(default_factory=lambda: os.path.expanduser(os.getenv('PROJECT_HOME', '~/ibdn')))
    db_mode: str = field(default_factory=lambda: os.getenv('DB_MODE', 'cassandra'))
    minio_access_key: str = field(default_factory=lambda: os.getenv('MINIO_ROOT_USER', 'admin'))
    minio_secret_key: str = field(default_factory=lambda: os.getenv('MINIO_ROOT_PASSWORD', 'password'))
    minio_endpoint: str = field(default_factory=lambda: os.getenv('MINIO_ENDPOINT', 'http://minio:9000'))
    spark_master: str = field(default_factory=lambda: os.getenv('SPARK_MASTER_URL', 'spark://spark-manager:7077'))
    prediction_jar: str = field(default_factory=lambda: os.getenv(
        'PREDICTION_JAR', '/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar'))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'))
    kafka_local_bootstrap: str = field(default_factory=lambda: os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092'))
    venv_python: str = field(default_factory=lambda: os.path.join(
        os.path.expanduser(os.getenv('PROJECT_HOME', '~/ibdn')), '.venv/bin/python3'))
    spark_container: str = field(default_factory=lambda: os.getenv('SPARK_CONTAINER', 'spark-manager'))
    kafka_container: str = field(default_factory=lambda: os.getenv('KAFKA_CONTAINER', 'kafka'))

    @staticmethod
    def from_env(db_mode=None):
        if db_mode:
            os.environ['DB_MODE'] = db_mode
        return DeployConfig()
