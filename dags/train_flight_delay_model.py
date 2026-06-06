from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import mlflow
import os
import requests as req
import time

MIN_ACCURACY = 0.85
MODEL_NAME = "FlightDelayRF"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
JAR = "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"


def _wait_for_spark(**context):
    """Wait for Spark master to be ready."""
    for _ in range(24):
        try:
            r = req.get("http://spark-manager:8080/json/", timeout=5)
            if r.status_code == 200:
                print("Spark master is ready")
                return
        except Exception:
            pass
        time.sleep(10)
    raise Exception("Spark master did not become ready within 4 minutes")


def _train_model(**context):
    import time as _time, json as _json
    access_key = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    # Detect if Docker is available (local) or we need kubectl/Spark REST API (GKE)
    use_docker = subprocess.run(
        "docker info >/dev/null 2>&1", shell=True
    ).returncode == 0

    if use_docker:
        cmd = (
            f"docker exec spark-manager spark-submit --master spark://spark-manager:7077 "
            f"--deploy-mode cluster --conf spark.cores.max=2 "
            f"--conf spark.hadoop.fs.s3a.access.key={access_key} "
            f"--conf spark.hadoop.fs.s3a.secret.key={secret_key} "
            f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            f"--conf spark.hadoop.fs.s3a.path.style.access=true "
            f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
        f"--conf spark.executorEnv.MLFLOW_TRACKING_URI={mlflow_uri} "
        f"--conf spark.driverEnv.MODEL_VERSION={os.getenv('MODEL_VERSION', '1.0')} "
        f"--conf spark.driverEnv.BUCKETIZER_VERSION={os.getenv('BUCKETIZER_VERSION', '1.0')} "
        f"--conf spark.driver.extraJavaOptions=--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        f"--class es.upm.dit.ging.predictor.TrainModel {JAR}"
        )
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=1800)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
    else:
        # GKE: use Spark REST API via kubectl exec
        spark_conf = (
            f"spark.master=spark://spark-manager:7077,"
            f"spark.submit.deployMode=cluster,"
            f"spark.cores.max=2,"
            f"spark.driver.memory=2g,"
            f"spark.executor.memory=2g,"
            f"spark.hadoop.fs.s3a.access.key={access_key},"
            f"spark.hadoop.fs.s3a.secret.key={secret_key},"
            f"spark.hadoop.fs.s3a.endpoint=http://minio:9000,"
            f"spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem,"
            f"spark.hadoop.fs.s3a.path.style.access=true,"
            f"spark.hadoop.fs.s3a.connection.ssl.enabled=false,"
            f"spark.executorEnv.MLFLOW_TRACKING_URI={mlflow_uri}"
        )
        payload = _json.dumps({
            "action": "CreateSubmissionRequest",
            "appArgs": [],
            "appResource": f"file:{JAR}",
            "clientSparkVersion": "4.1.1",
            "environmentVariables": {"MLFLOW_TRACKING_URI": mlflow_uri},
            "mainClass": "es.upm.dit.ging.predictor.TrainModel",
            "sparkProperties": dict(item.split("=", 1) for item in spark_conf.split(",")),
        })
        r = req.post("http://spark-manager:6066/v1/submissions/create",
                     data=payload, headers={"Content-Type": "application/json"}, timeout=10)
        print(f"Spark REST submit: {r.status_code} {r.text[:300]}")

    # Wait for the Spark app to appear and then finish
    for _ in range(120):
        _time.sleep(5)
        try:
            r = req.get("http://spark-manager:8080/json/", timeout=5)
            data = r.json()
            active = [a for a in data.get("activeapps", [])
                      if "train_spark_mllib_model" in a.get("name", "")
                      and a.get("state") not in ("FINISHED", "KILLED", "FAILED")]
            if not active:
                completed = [a for a in data.get("completedapps", [])
                             if "train_spark_mllib_model" in a.get("name", "")]
                if completed:
                    print(f"Training job finished: state={completed[0].get('state')}")
                    return
                continue
            print(f"Training still running ({active[0].get('duration', 0)//1000}s)...")
        except Exception as e:
            print(f"Poll error: {e}")
    raise Exception("Timed out waiting for training job to finish")


def _check_and_register(**context):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    runs = client.search_runs(
        experiment_ids=["0"],
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        print("No runs found in MLflow experiment 0")
        return

    run = runs[0]
    run_id = run.info.run_id
    accuracy = run.data.metrics.get("accuracy", 0)
    print(f"Run: {run_id}, Accuracy: {accuracy:.4f}, Threshold: {MIN_ACCURACY}")

    if accuracy < MIN_ACCURACY:
        print(f"Accuracy {accuracy:.4f} below threshold {MIN_ACCURACY}. Not registering.")
        return

    model_uri = f"runs:/{run_id}/model"
    try:
        mv = mlflow.register_model(model_uri, MODEL_NAME)
        print(f"Model registered: {MODEL_NAME} v{mv.version}")
        client.transition_model_version_stage(
            name=MODEL_NAME, version=mv.version, stage="Production"
        )
        print("Promoted to Production")

        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        for v in versions:
            if v.current_stage == "Production" and v.version != mv.version:
                client.transition_model_version_stage(
                    name=MODEL_NAME, version=v.version, stage="Archived"
                )
                print(f"Archived previous Production model v{v.version}")
    except Exception as e:
        print(f"Model has no 'model' artifact — Spark saves to MinIO directly: {e}")


with DAG(
    dag_id="train_flight_delay_model",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Entrena el modelo Random Forest de prediccion de vuelos y lo promueve en MLflow Registry",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "spark"],
) as dag:

    spark_ready = PythonOperator(
        task_id="spark_ready",
        python_callable=_wait_for_spark,
    )

    train_model = PythonOperator(
        task_id="train_model",
        python_callable=_train_model,
    )

    check_and_register = PythonOperator(
        task_id="check_and_register",
        python_callable=_check_and_register,
    )

    spark_ready >> train_model >> check_and_register
