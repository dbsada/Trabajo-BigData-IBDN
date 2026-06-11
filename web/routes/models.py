"""Gestión de modelos: listar, activar, borrar, entrenar."""

import os
import json
import threading
import time

import boto3
import requests
from botocore.config import Config
from flask import Blueprint, jsonify, request

bp = Blueprint("models", __name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "password")
MLFLOW_URI = os.getenv("MLFLOW_URI", "http://mlflow:5000")

_training_lock = threading.Lock()
_training_active = False
_training_start_time = 0
_training_app_id = None
_emit_training = lambda data: None


def set_emit_function(fn):
    global _emit_training
    _emit_training = fn


def _get_minio():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version="s3v4"),
    )


@bp.route("/api/models")
def list_models():
    """Devuelve todos los modelos desde MLflow + los modelos activos."""
    run_id_to_registry = {}
    runs_data = []

    def fetch_registry():
        try:
            resp = requests.post(
                f"{MLFLOW_URI}/api/2.0/mlflow/registered-models/search",
                json={"max_results": 100},
                timeout=5,
            )
            for rm in resp.json().get("registered_models", []):
                for mv in rm.get("latest_versions", []):
                    run_id_to_registry[mv["run_id"]] = {
                        "name": rm["name"],
                        "version": mv["version"],
                        "stage": mv.get("current_stage", "None"),
                    }
        except Exception:
            pass

    def fetch_runs():
        try:
            resp = requests.post(
                f"{MLFLOW_URI}/api/2.0/mlflow/runs/search",
                json={"experiment_ids": ["0"], "order_by": ["start_time desc"]},
                timeout=5,
            )
            runs_data.extend(
                r
                for r in resp.json().get("runs", [])
                if r.get("info", {}).get("lifecycle_stage", "active") != "deleted"
            )
        except Exception:
            pass

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2) as pool:
        pool.submit(fetch_registry)
        pool.submit(fetch_runs)

    model_list = []

    for run in runs_data:
        info = run["info"]
        data = run["data"]
        run_id = info["run_id"]
        params = {p["key"]: p["value"] for p in data.get("params", [])}
        metrics = {m["key"]: m["value"] for m in data.get("metrics", [])}
        tags = {t["key"]: t["value"] for t in data.get("tags", [])}
        reg = run_id_to_registry.get(run_id, {})
        # Usar model_version tag como run_id si existe (necesario para MakePrediction)
        display_id = tags.get("model_version", run_id)
        model_list.append({
            "run_id": display_id,
            "run_name": info.get("run_name", display_id[:8]),
            "status": info["status"],
            "start_time": info.get("start_time", 0),
            "params": params,
            "metrics": metrics,
            "stage": reg.get("stage", "None"),
            "version": reg.get("version"),
            "registered_name": reg.get("name"),
            "verified": tags.get("verified", "false") == "true",
        })

    # Fallback: añadir modelos de MinIO que aún no estén en MLflow
    mlflow_ids = {m["run_id"] for m in model_list}
    try:
        s3 = _get_minio()
        objs = s3.list_objects_v2(
            Bucket="lakehouse",
            Prefix="models/spark_random_forest_classifier.flight_delays.",
        )
        seen = {}
        for o in objs.get("Contents", []):
            key = o["Key"]
            parts = key.split("/")
            if len(parts) < 2:
                continue
            dir_parts = parts[1].split(".")
            if len(dir_parts) >= 3 and dir_parts[-1] == "bin":
                uid = dir_parts[-2]
                ts = o["LastModified"].timestamp()
                if uid not in mlflow_ids and (uid not in seen or ts > seen[uid][1]):
                    seen[uid] = (uid, ts, o["LastModified"])
        for uid, ts, lastmod in sorted(seen.values(), key=lambda x: x[1], reverse=True):
            model_list.append({
                "run_id": uid,
                "run_name": uid[:8],
                "status": "FINISHED",
                "start_time": int(lastmod.timestamp() * 1000),
                "params": {},
                "metrics": {},
                "stage": "None",
                "version": None,
                "registered_name": None,
            })
    except Exception:
        pass

    # Leer modelos activos desde MinIO (uno por línea)
    active_model_ids = []
    try:
        marker = _get_minio().get_object(
            Bucket="lakehouse", Key="models/active_run_id.txt"
        )
        content = marker["Body"].read().decode().strip()
        for line in content.split("\n"):
            line = line.strip()
            if line:
                active_model_ids.append(line)
    except Exception:
        pass

    return jsonify({"models": model_list, "active_model_ids": active_model_ids})


@bp.route("/api/models/activate/<run_id>", methods=["POST"])
def activate_model(run_id):
    """Añade un modelo a la lista de activos."""
    ids = []
    try:
        marker = _get_minio().get_object(
            Bucket="lakehouse", Key="models/active_run_id.txt"
        )
        for line in marker["Body"].read().decode().strip().split("\n"):
            if line.strip() and line.strip() != run_id:
                ids.append(line.strip())
    except Exception:
        pass
    ids.append(run_id)

    _get_minio().put_object(
        Bucket="lakehouse",
        Key="models/active_run_id.txt",
        Body="\n".join(ids).encode(),
    )
    return jsonify({"ok": True, "active_model_ids": ids})


@bp.route("/api/models/deactivate/<run_id>", methods=["POST"])
def deactivate_model(run_id):
    """Elimina un modelo de la lista de activos."""
    ids = []
    try:
        marker = _get_minio().get_object(
            Bucket="lakehouse", Key="models/active_run_id.txt"
        )
        for line in marker["Body"].read().decode().strip().split("\n"):
            if line.strip() and line.strip() != run_id:
                ids.append(line.strip())
    except Exception:
        pass

    _get_minio().put_object(
        Bucket="lakehouse",
        Key="models/active_run_id.txt",
        Body="\n".join(ids).encode(),
    )
    return jsonify({"ok": True, "active_model_ids": ids})


@bp.route("/api/models/delete/<run_id>", methods=["POST"])
def delete_model(run_id):
    """Borra un modelo de MLflow y de MinIO."""
    errors = []

    # Buscar MLflow run_id real a partir del model_version tag
    mlflow_run_id = None
    try:
        resp = requests.post(
            f"{MLFLOW_URI}/api/2.0/mlflow/runs/search",
            json={"experiment_ids": ["0"], "order_by": ["start_time desc"]},
            timeout=5,
        )
        for r in resp.json().get("runs", []):
            tags = {t["key"]: t["value"] for t in r["data"].get("tags", [])}
            if tags.get("model_version") == run_id:
                mlflow_run_id = r["info"]["run_id"]
                break
    except Exception:
        pass

    if mlflow_run_id:
        try:
            requests.post(
                f"{MLFLOW_URI}/api/2.0/mlflow/runs/delete",
                json={"run_id": mlflow_run_id},
                timeout=5,
            )
        except Exception as e:
            errors.append(str(e))
        try:
            s3 = _get_minio()
            objs = s3.list_objects_v2(
                Bucket="lakehouse", Prefix=f"mlflow/0/{mlflow_run_id}/"
            )
            for obj in objs.get("Contents", []):
                s3.delete_object(Bucket="lakehouse", Key=obj["Key"])
        except Exception as e:
            errors.append(str(e))

    # Borrar modelo de MinIO (spark ML)
    try:
        s3 = _get_minio()
        objs = s3.list_objects_v2(
            Bucket="lakehouse",
            Prefix=f"models/spark_random_forest_classifier.flight_delays.{run_id}.bin/",
        )
        for obj in objs.get("Contents", []):
            s3.delete_object(Bucket="lakehouse", Key=obj["Key"])
        s3.delete_object(Bucket="lakehouse", Key=f"models/accuracy.{run_id}.json")
        s3.delete_object(Bucket="lakehouse", Key=f"models/dag_run_{run_id}.txt")
    except Exception as e:
        errors.append(str(e))

    # Desactivar si estaba activo
    try:
        ids = []
        marker = _get_minio().get_object(
            Bucket="lakehouse", Key="models/active_run_id.txt"
        )
        for line in marker["Body"].read().decode().strip().split("\n"):
            if line.strip() and line.strip() != run_id:
                ids.append(line.strip())
        _get_minio().put_object(
            Bucket="lakehouse",
            Key="models/active_run_id.txt",
            Body="\n".join(ids).encode(),
        )
    except Exception:
        pass

    return jsonify({"ok": True, "errors": errors if errors else None})


# ═══════════════════════════════════════════════════════════════
# TRAINING
# ═══════════════════════════════════════════════════════════════

def _build_spark_props_json():
    conf_items = [
        '"spark.master":"spark://spark-manager:7077"',
        '"spark.submit.deployMode":"cluster"',
        '"spark.cores.max":"4"',
        '"spark.executor.memory":"2g"',
        f'"spark.hadoop.fs.s3a.access.key":"{MINIO_USER}"',
        f'"spark.hadoop.fs.s3a.secret.key":"{MINIO_PASSWORD}"',
        f'"spark.hadoop.fs.s3a.endpoint":"{MINIO_ENDPOINT}"',
        '"spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"',
        '"spark.sql.catalog.lakehouse":"org.apache.iceberg.spark.SparkCatalog"',
        '"spark.sql.catalog.lakehouse.type":"hadoop"',
        '"spark.sql.catalog.lakehouse.warehouse":"s3a://lakehouse"',
        f'"spark.sql.catalog.lakehouse.s3.endpoint":"{MINIO_ENDPOINT}"',
        f'"spark.sql.catalog.lakehouse.s3.access-key":"{MINIO_USER}"',
        f'"spark.sql.catalog.lakehouse.s3.secret-key":"{MINIO_PASSWORD}"',
        '"spark.sql.defaultCatalog":"lakehouse"',
        '"spark.hadoop.fs.s3a.path.style.access":"true"',
        '"spark.hadoop.fs.s3a.list.version":"1"',
        '"spark.hadoop.fs.s3a.connection.ssl.enabled":"false"',
        f'"spark.driverEnv.MLFLOW_TRACKING_URI":"{MLFLOW_URI}"',
        f'"spark.executorEnv.MLFLOW_TRACKING_URI":"{MLFLOW_URI}"',
        '"spark.driver.extraJavaOptions":"--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"',
    ]
    return ",".join(conf_items)


def _find_latest_model_in_minio():
    """Busca el modelo más reciente en MinIO por fecha de modificación."""
    try:
        s3 = _get_minio()
        objs = s3.list_objects_v2(
            Bucket="lakehouse",
            Prefix="models/spark_random_forest_classifier.flight_delays.",
        )
        versions = {}
        for o in objs.get("Contents", []):
            key = o["Key"]
            parts = key.split("/")
            if len(parts) < 2:
                continue
            model_dir = parts[1]
            dir_parts = model_dir.split(".")
            if len(dir_parts) >= 3 and dir_parts[-1] == "bin":
                uid = dir_parts[-2]
                ts = o["LastModified"].timestamp()
                if uid not in versions or ts > versions[uid][1]:
                    versions[uid] = (uid, ts)
        if versions:
            latest = max(versions.values(), key=lambda x: x[1])
            return latest[0]
    except Exception:
        pass
    return None


def _register_mlflow_run(version, run_name, num_trees, max_depth, duration_seconds):
    """Crea un run en MLflow con los parámetros del training.
    Devuelve el version UUID (Scala) que es el que usa MakePrediction."""
    try:
        now = int(time.time() * 1000)
        resp = requests.post(
            f"{MLFLOW_URI}/api/2.0/mlflow/runs/create",
            json={"experiment_id": "0", "start_time": now, "run_name": run_name},
            timeout=5,
        )
        run_id = resp.json()["run"]["info"]["run_id"]
        requests.post(
            f"{MLFLOW_URI}/api/2.0/mlflow/runs/set-tag",
            json={"run_id": run_id, "key": "model_version", "value": version},
            timeout=5,
        )
        for k, v in [
            ("num_trees", str(num_trees)),
            ("max_depth", str(max_depth)),
            ("training_duration_seconds", str(duration_seconds)),
        ]:
            requests.post(
                f"{MLFLOW_URI}/api/2.0/mlflow/runs/log-parameter",
                json={"run_id": run_id, "key": k, "value": v},
                timeout=5,
            )
        # Leer precisión desde MinIO (guardada por TrainModel.scala)
        try:
            s3 = _get_minio()
            acc_obj = s3.get_object(
                Bucket="lakehouse", Key=f"models/accuracy.{version}.json"
            )
            acc_data = json.loads(acc_obj["Body"].read())
            accuracy = float(acc_data.get("accuracy", 0))
            requests.post(
                f"{MLFLOW_URI}/api/2.0/mlflow/runs/log-metric",
                json={"run_id": run_id, "key": "accuracy", "value": accuracy, "timestamp": int(time.time() * 1000)},
                timeout=5,
            )
        except Exception:
            pass
        requests.post(
            f"{MLFLOW_URI}/api/2.0/mlflow/runs/update",
            json={"run_id": run_id, "status": "FINISHED", "end_time": int(time.time() * 1000)},
            timeout=5,
        )
    except Exception:
        pass
    return version


def _run_training(num_trees, max_depth, run_name):
    global _training_active, _training_start_time, _training_app_id
    _training_active = True
    _training_start_time = time.time()
    _emit_training({"status": "started", "run_name": run_name})

    jar_path = "/app/spark-jobs/target/scala-2.13/spark-jobs_2.13-0.1.jar"
    props_json = _build_spark_props_json()
    model_version = None

    try:
        payload = (
            '{"action":"CreateSubmissionRequest","appArgs":'
            f'["--num-trees","{num_trees}","--max-depth","{max_depth}","--run-name","{run_name}"],'
            f'"appResource":"file:{jar_path}","clientSparkVersion":"4.1.1",'
            '"mainClass":"TrainModel",'
            f'"sparkProperties":{{{props_json}}}}}'
        )
        resp = requests.post(
            "http://spark-manager:6066/v1/submissions/create",
            data=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        sub_data = resp.json()
        if not sub_data.get("success", False):
            raise Exception(f"Spark submission failed: {sub_data.get('message', sub_data)}")
        _training_app_id = sub_data.get("submissionId")
        print(f"Spark submission accepted: {_training_app_id}")

        while _training_active:
            status_resp = requests.get(
                f"http://spark-manager:6066/v1/submissions/status/{_training_app_id}",
                timeout=5,
            )
            status_json = status_resp.json()
            s = status_json.get("driverState", "")
            if s in ("FINISHED", "FAILED", "KILLED"):
                if s == "FINISHED":
                    version = _find_latest_model_in_minio()
                    if version:
                        dur = int(time.time() - _training_start_time)
                        model_version = _register_mlflow_run(version, run_name, num_trees, max_depth, dur)
                elif s == "FAILED":
                    error_msg = status_json.get("message", "Spark training job failed")
                    raise Exception(f"Spark training job failed: {error_msg}")
                break
            elapsed = int(time.time() - _training_start_time)
            _emit_training({"status": "running", "elapsed": elapsed, "run_name": run_name})
            time.sleep(3)

        _emit_training({
            "status": "completed" if model_version else "failed",
            "model_version": model_version,
            "run_name": run_name,
            "elapsed": int(time.time() - _training_start_time),
        })

    except Exception as e:
        _emit_training({
            "status": "failed",
            "error": str(e),
            "run_name": run_name,
            "elapsed": int(time.time() - _training_start_time),
        })
    finally:
        _training_active = False
        _training_app_id = None


@bp.route("/api/models/train", methods=["POST"])
def api_train_model():
    global _training_active
    with _training_lock:
        if _training_active:
            return jsonify({"status": "already_running"}), 200

        data = request.get_json(silent=True) or {}
        num_trees = int(data.get("num_trees", 20))
        max_depth = int(data.get("max_depth", 10))
        run_name = str(data.get("run_name", "rf_training")).strip() or "rf_training"

        _training_active = True
        t = threading.Thread(
            target=_run_training,
            args=(num_trees, max_depth, run_name),
            daemon=True,
        )
        t.start()
        return jsonify({"status": "started"}), 200


@bp.route("/api/models/train/status")
def api_train_status():
    elapsed = int(time.time() - _training_start_time) if _training_active else 0
    if _training_active:
        return jsonify({"status": "running", "elapsed": elapsed})
    return jsonify({"status": "idle", "elapsed": 0})


@bp.route("/api/models/train/cancel", methods=["POST"])
def api_train_cancel():
    global _training_active, _training_app_id
    with _training_lock:
        if not _training_active:
            return jsonify({"ok": True, "message": "no training running"})
        _training_active = False
        if _training_app_id:
            try:
                requests.post(
                    f"http://spark-manager:6066/v1/submissions/kill/{_training_app_id}",
                    timeout=5,
                )
            except Exception:
                pass
        _emit_training({"status": "cancelled"})
    return jsonify({"ok": True})