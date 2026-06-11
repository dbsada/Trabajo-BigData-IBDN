from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
import subprocess
import json
import os
import time as _time
import boto3
import requests as req
from botocore.config import Config

MIN_ACCURACY = 0.65
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
JAR = "/app/spark-jobs/target/scala-2.13/spark-jobs_2.13-0.1.jar"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password")


def _get_minio():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version="s3v4"),
    )


def _wait_for_spark(**context):
    """Wait for Spark master to be ready."""
    for _ in range(48):
        r = subprocess.run(
            ["curl", "-s", "--connect-timeout", "3", "--max-time", "5",
             "http://spark-manager:8080/json/"],
            capture_output=True, text=True, timeout=10)
        if r.returncode == 0:
            print("Spark master is ready")
            _time.sleep(5)
            return
        _time.sleep(3)
    raise Exception("Spark master did not become ready within 2.5 minutes")


def _train_model(**context):
    access_key = MINIO_USER
    secret_key = MINIO_PASSWORD
    mlflow_uri = MLFLOW_URI

    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    num_trees = conf.get("num_trees", 20)
    max_depth = conf.get("max_depth", 10)
    run_name = conf.get("run_name", "").strip() or "rf_training"

    print(f"Training: num_trees={num_trees}, max_depth={max_depth}, run_name={run_name}")

    cmd = (
        f"docker exec spark-manager spark-submit --master spark://spark-manager:7077 "
        f"--deploy-mode cluster --conf spark.cores.max=4 --conf spark.executor.memory=2g "
        f"--conf 'spark.hadoop.fs.s3a.endpoint=http://minio:9000' "
        f"--conf 'spark.hadoop.fs.s3a.access.key={access_key}' "
        f"--conf 'spark.hadoop.fs.s3a.secret.key={secret_key}' "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
        f"--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        f"--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog "
        f"--conf spark.sql.catalog.lakehouse.type=hadoop "
        f"--conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse "
        f"--conf spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000 "
        f"--conf spark.sql.catalog.lakehouse.s3.access-key={access_key} "
        f"--conf spark.sql.catalog.lakehouse.s3.secret-key={secret_key} "
        f"--conf spark.sql.defaultCatalog=lakehouse "
        f"--conf spark.executorEnv.MLFLOW_TRACKING_URI={mlflow_uri} "
        f"--conf spark.driverEnv.MODEL_VERSION=1.0 "
        f"--conf spark.driverEnv.BUCKETIZER_VERSION=1.0 "
        f"--conf spark.driver.extraJavaOptions=--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        f"--class TrainModel {JAR} "
        f"--num-trees {num_trees} --max-depth {max_depth} --run-name {run_name}"
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=1800)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    # Esperar a que el job termine en Spark
    for _ in range(900):
        _time.sleep(2)
        try:
            r = req.get("http://spark-manager:8080/json/", timeout=2)
            data = r.json()
            active = [a for a in data.get("activeapps", [])
                      if "TrainRandomForest" in a.get("name", "")
                      and a.get("state") not in ("FINISHED", "KILLED", "FAILED")]
            if not active:
                completed = [a for a in data.get("completedapps", [])
                             if "TrainRandomForest" in a.get("name", "")]
                if completed:
                    print(f"Training job finished")
                    break
                continue
            print(f"Training running ({active[0].get('duration', 0)//1000}s)...")
        except Exception as e:
            print(f"Poll error: {e}")
    else:
        raise Exception("Timed out waiting for training job to finish")

    # Leer last_trained_version.txt de MinIO
    for _ in range(30):
        _time.sleep(2)
        try:
            s3 = _get_minio()
            obj = s3.get_object(Bucket="lakehouse", Key="models/last_trained_version.txt")
            model_version = obj["Body"].read().decode().strip()
            if model_version:
                # Guardar en MinIO con el run_id del DAG para evitar colisiones
                s3.put_object(
                    Bucket="lakehouse",
                    Key=f"models/dag_run_{run_id}.txt",
                    Body=model_version.encode(),
                )
                ti = context["ti"]
                ti.xcom_push(key="model_version", value=model_version)
                print(f"Model version: {model_version}")
                print(f"Saved to models/dag_run_{run_id}.txt")
                _time.sleep(5)
                return
        except Exception as e:
            print(f"Waiting for last_trained_version.txt: {e}")
    raise Exception("Could not read last_trained_version.txt from MinIO")


def _check_and_register(**context):
    run_id = context["dag_run"].run_id.replace(":", "_").replace("+", "_")
    s3 = _get_minio()
    model_version = None
    for _ in range(30):
        _time.sleep(2)
        try:
            obj = s3.get_object(Bucket="lakehouse", Key=f"models/dag_run_{run_id}.txt")
            model_version = obj["Body"].read().decode().strip()
            if model_version:
                print(f"Model version from MinIO: {model_version}")
                break
        except Exception as e:
            print(f"Waiting for dag_run_{run_id}.txt: {e}")
    if not model_version:
        # Fallback: XCom
        ti = context["ti"]
        model_version = ti.xcom_pull(key="model_version", task_ids="train_model")
        if not model_version:
            raise Exception("Could not read model_version")

    # Leer accuracy JSON de MinIO
    s3 = _get_minio()
    obj = s3.get_object(Bucket="lakehouse", Key=f"models/accuracy.{model_version}.json")
    acc_data = json.loads(obj["Body"].read())
    accuracy = float(acc_data["accuracy"])
    num_trees = acc_data.get("num_trees", "?")
    max_depth = acc_data.get("max_depth", "?")
    run_name = acc_data.get("run_name", model_version[:8])

    print(f"Model: {model_version[:12]}, accuracy={accuracy:.4f}, trees={num_trees}, depth={max_depth}")

    # Crear MLflow run
    now_ms = int(_time.time() * 1000)
    resp = req.post(
        f"{MLFLOW_URI}/api/2.0/mlflow/runs/create",
        json={"experiment_id": "0", "start_time": now_ms, "run_name": run_name[:8]},
        timeout=5,
    )
    run_id = resp.json()["run"]["info"]["run_id"]
    print(f"MLflow run created: {run_id[:12]}")

    # Set tag model_version
    req.post(
        f"{MLFLOW_URI}/api/2.0/mlflow/runs/set-tag",
        json={"run_id": run_id, "key": "model_version", "value": model_version},
        timeout=5,
    )

    # Log params
    for k, v in [("num_trees", str(num_trees)), ("max_depth", str(max_depth)),
                  ("training_duration_seconds", str(acc_data.get("training_duration_seconds", "?")))]:
        req.post(
            f"{MLFLOW_URI}/api/2.0/mlflow/runs/log-parameter",
            json={"run_id": run_id, "key": k, "value": v},
            timeout=5,
        )

    # Log metric: accuracy
    req.post(
        f"{MLFLOW_URI}/api/2.0/mlflow/runs/log-metric",
        json={"run_id": run_id, "key": "accuracy", "value": accuracy, "timestamp": now_ms},
        timeout=5,
    )

    # Set verified tag
    verified = str(accuracy >= MIN_ACCURACY).lower()
    req.post(
        f"{MLFLOW_URI}/api/2.0/mlflow/runs/set-tag",
        json={"run_id": run_id, "key": "verified", "value": verified},
        timeout=5,
    )
    print(f"Verified={verified} (accuracy {accuracy:.4f} {'>=' if accuracy >= MIN_ACCURACY else '<'} {MIN_ACCURACY})")

    # Update run status to FINISHED
    req.post(
        f"{MLFLOW_URI}/api/2.0/mlflow/runs/update",
        json={"run_id": run_id, "status": "FINISHED", "end_time": int(_time.time() * 1000)},
        timeout=5,
    )
    print(f"Done. Run {run_id[:12]} registered with verified={verified}")
    _time.sleep(5)


@dag(
    dag_id="train_random_forest",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Entrena Random Forest via Spark, crea MLflow run y verifica accuracy",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "spark", "rf"],
)
def train_random_forest():

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


dag = train_random_forest()
