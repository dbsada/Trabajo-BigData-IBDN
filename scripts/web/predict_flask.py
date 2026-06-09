import sys, os, re, logging
from flask import Flask, render_template, request, redirect, url_for
import json
import socket
import time
import json
import threading
import iso8601
import datetime

from flask_socketio import SocketIO, emit, join_room

import config
import predict_utils

static_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
app = Flask(__name__, static_folder=static_folder)
socketio = SocketIO(app, cors_allowed_origins="*")

# Suppress Werkzeug access logs for /api/logs/ endpoints
class LogFilter(logging.Filter):
    def filter(self, record):
        return '/api/logs/' not in record.getMessage()

logging.getLogger('werkzeug').addFilter(LogFilter())

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_LOCAL_BOOTSTRAP_SERVERS = os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'flight-delay-ml-request')
KAFKA_RESPONSE_TOPIC = os.getenv('KAFKA_RESPONSE_TOPIC', 'flight-delay-ml-response')
KAFKA_STATUS_TOPIC = os.getenv('KAFKA_STATUS_TOPIC', 'flight-delay-ml-status')

# Cassandra-only: stub to avoid breaking legacy routes
class _StubDB:
    def find(self, *a, **kw): return []
    def find_one(self, *a, **kw): return None
    def count_documents(self, *a, **kw): return 0
    def insert_one(self, *a, **kw): pass
    def sort(self, *a, **kw): return self
    def skip(self, *a, **kw): return self
    def limit(self, *a, **kw): return self
    def __getitem__(self, key): return self
    def __getattr__(self, name): return self
_db_stub = _StubDB()
def _get_db():
    return _db_stub

@app.context_processor
def inject_arch_info():
    host = None
    try:
        host = __import__('subprocess').run(
            ['hostname', '-I'], capture_output=True, text=True
        ).stdout.strip().split()[0]
    except:
        pass
    host = host or 'localhost'
    deploy_mode = os.getenv('DEPLOY_MODE', 'local')
    if deploy_mode == 'gke':
        deploy_label = '⚡ GKE'
        deploy_style = 'background:#fdc41b20;color:#fdc41b;border:1px solid #fdc41b40'
    else:
        deploy_label = '🖥️ Local'
        deploy_style = 'background:#22c55e20;color:#22c55e;border:1px solid #22c55e40'
    return dict(
        VM_IP=host,
        DB_MODE=os.getenv('DB_MODE', 'cassandra'),
        DEPLOY_MODE=deploy_mode,
        KAFKA_TOPIC=os.getenv('KAFKA_TOPIC', 'flight-delay-ml-request'),
        KAFKA_RESPONSE_TOPIC=os.getenv('KAFKA_RESPONSE_TOPIC', 'flight-delay-ml-response'),
        DEPLOY_LABEL=deploy_label,
        DEPLOY_STYLE=deploy_style,
    )

@app.template_filter()
def timestamp_to_date(ts):
    import datetime
    return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M')

_airport_cache = None

@app.route("/api/airports")
def api_airports():
    global _airport_cache
    if _airport_cache is None:
        import bz2, json
        codes = set()
        with bz2.open('/app/data/simple_flight_delay_features.jsonl.bz2', 'rt') as f:
            for line in f:
                r = json.loads(line)
                codes.add(r.get('Origin'))
                codes.add(r.get('Dest'))
        _airport_cache = sorted(codes)
    q = request.args.get('q', '').upper()
    match = [a for a in _airport_cache if a.startswith(q)] if q else _airport_cache
    return json.dumps(match[:50])

_minio_client = None

def _get_minio_client():
    global _minio_client
    if _minio_client is None:
        import boto3
        from botocore.config import Config
        _minio_client = boto3.client("s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
            config=Config(signature_version="s3v4"))
    return _minio_client


@app.route("/api/models")
def api_models():
    import requests
    from concurrent.futures import ThreadPoolExecutor

    MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    run_id_to_registry = {}
    runs_data = []

    def _fetch_registry():
        nonlocal run_id_to_registry
        try:
            resp = requests.post(f"{MLFLOW_URI}/api/2.0/mlflow/registered-models/search",
                json={"max_results": 100}, timeout=5)
            for rm in resp.json().get("registered_models", []):
                for mv in rm.get("latest_versions", []):
                    run_id_to_registry[mv["run_id"]] = {
                        "name": rm["name"],
                        "version": mv["version"],
                        "stage": mv.get("current_stage", "None"),
                    }
        except Exception:
            pass

    def _fetch_runs():
        nonlocal runs_data
        try:
            resp = requests.post(f"{MLFLOW_URI}/api/2.0/mlflow/runs/search",
                json={"experiment_ids": ["0"], "order_by": ["start_time desc"]}, timeout=5)
            runs_data = resp.json().get("runs", [])
            runs_data = [r for r in runs_data if r.get("info", {}).get("lifecycle_stage", "active") != "deleted"]
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=2) as pool:
        pool.submit(_fetch_registry)
        pool.submit(_fetch_runs)

    model_list = []
    for run in runs_data:
        info = run["info"]
        data = run["data"]
        run_id = info["run_id"]
        params = {p["key"]: p["value"] for p in data.get("params", [])}
        metrics = {m["key"]: m["value"] for m in data.get("metrics", [])}
        reg = run_id_to_registry.get(run_id, {})
        model_list.append({
            "run_id": run_id,
            "run_name": info.get("run_name", run_id[:8]),
            "status": info["status"],
            "start_time": info.get("start_time", 0) or info.get("end_time", 0) or int(time.time() * 1000),
            "params": params,
            "metrics": metrics,
            "stage": reg.get("stage", "None"),
            "version": reg.get("version"),
            "registered_name": reg.get("name"),
        })

    active_run_id = None
    for m in model_list:
        if m.get("stage") == "Production":
            active_run_id = m["run_id"]
            break

    if not active_run_id:
        try:
            marker = _get_minio_client().get_object(Bucket="lakehouse", Key="models/active_run_id.txt")
            active_run_id = marker["Body"].read().decode().strip()
        except Exception:
            active_run_id = None

    return json.dumps({"models": model_list, "active_run_id": active_run_id})

@app.route("/api/models/activate/<run_id>", methods=["POST"])
def api_activate_model(run_id):
    import boto3, os, requests as req
    from botocore.config import Config

    MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    MODEL_NAME = "FlightDelayRF"
    deploy_mode = os.getenv("DEPLOY_MODE", "")

    errors = []

    try:
        if deploy_mode == "gke":
            import boto3 as s3boto
            from botocore.config import Config as S3Config
            s3 = s3boto.client("s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
                config=S3Config(signature_version="s3v4"))
            s3.put_object(Bucket="lakehouse", Key="models/active_run_id.txt", Body=run_id.encode())
        else:
            import subprocess
            marker_path = "local/lakehouse/models/active_run_id.txt"
            subprocess.run(
                ["docker", "exec", "minio", "mc", "rm", "--recursive", "--force", marker_path],
                capture_output=True
            )
            subprocess.run(
                ["docker", "exec", "-i", "minio", "mc", "pipe", marker_path],
                input=run_id.encode(), check=True, capture_output=True
            )
    except Exception as e:
        errors.append(str(e))

    threading.Thread(target=_restart_prediction_job, daemon=True).start()
    return json.dumps({"ok": True, "active_run_id": run_id, "errors": errors if errors else None})

_pipeline_state_file = '/tmp/pipeline_state.json'
_pipeline_state_lock = threading.Lock()

def _load_pipeline_state():
    try:
        with open(_pipeline_state_file) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_pipeline_state(state):
    with open(_pipeline_state_file, 'w') as f:
        json.dump(state, f)

def _detect_pipeline_state(state):
    """Auto-detect pipeline progress by checking infrastructure state.
    Only fills in infrastructure-detected steps if the pipeline has already
    started (at least one step was reported via POST), to avoid showing
    stale data from previous deploys as green."""
    try:
        import requests as _req
        now = time.time()

        # Always mark core_services as done (Flask is running)
        state.setdefault("core_services", {"status": "done", "message": "Flask running", "ts": now})
        state["core_services"]["status"] = "done"

        # Don't auto-detect pipeline steps unless core_services is done
        if not _is_done(state, "core_services"):
            return state

        # Check infra services (MinIO + Spark)
        infra_ok = True
        try:
            _req.get("http://minio:9000/minio/health/live", timeout=3)
        except Exception:
            infra_ok = False
        try:
            _req.get("http://spark-manager:8080/json/", timeout=3)
        except Exception:
            infra_ok = False
        if infra_ok and _safe_get(state, "infra_services").get("status") != "done":
            state["infra_services"] = {"status": "done", "message": "MinIO, Spark ready", "ts": now}

        # Check MinIO buckets/data via boto3
        try:
            from botocore.config import Config as _S3Config
            import boto3 as _s3
            s3 = _s3.client("s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
                config=_S3Config(signature_version="s3v4"))
            buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
            if "lakehouse" in buckets and _safe_get(state, "buckets").get("status") != "done":
                state["buckets"] = {"status": "done", "message": "MinIO buckets created", "ts": now}
            data_checks = [
                ("raw/simple_flight_delay_features.jsonl.bz2", "download", "Flight data downloaded"),
                ("raw/origin_dest_distances.jsonl", "import_distances", "Distance data imported"),
            ]
            for key, step_name, msg in data_checks:
                try:
                    s3.head_object(Bucket="lakehouse", Key=key)
                    if _safe_get(state, step_name).get("status") != "done":
                        state[step_name] = {"status": "done", "message": msg, "ts": now}
                except Exception:
                    pass
            both_exist = all(_is_done(state, sn) for _, sn, _ in data_checks)
            if both_exist and _safe_get(state, "upload").get("status") != "done":
                state["upload"] = {"status": "done", "message": "Data uploaded to MinIO", "ts": now}
        except Exception:
            pass

        # Check Kafka topics via admin client
        try:
            from kafka.admin import KafkaAdminClient as _KAC
            admin = _KAC(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), request_timeout_ms=3000)
            topics = admin.list_topics()
            admin.close()
            expected = ["flight-delay-ml-request", "flight-delay-ml-response", "flight-delay-ml-status"]
            if all(t in topics for t in expected) and _safe_get(state, "topics").get("status") != "done":
                state["topics"] = {"status": "done", "message": "Kafka topics created", "ts": now}
        except Exception:
            pass

        # Check Spark prediction
        try:
            from botocore.config import Config as _S3Config
            import boto3 as _s3
            _mc = _s3.client("s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
                config=_S3Config(signature_version="s3v4"))
            has_active_model = False
            try:
                _mc.head_object(Bucket="lakehouse", Key="models/active_run_id.txt")
                has_active_model = True
            except Exception:
                pass
            r = _req.get("http://spark-manager:8080/json/", timeout=5)
            j = r.json()
            has_prediction = False
            for app in j.get("activeapps", []):
                if "FlightDelayPrediction" in app.get("name", "") and app.get("state") not in ("FINISHED", "KILLED", "FAILED"):
                    has_prediction = True
                    break
            if not has_prediction:
                for dr in j.get("completeddrivers", []) + j.get("activedrivers", []):
                    if "MakePrediction" in dr.get("mainclass", ""):
                        has_prediction = True
                        break
            if has_prediction and _safe_get(state, "prediction").get("status") != "done":
                state["prediction"] = {"status": "done", "message": "Prediction engine started", "ts": now}
            elif not has_active_model and _safe_get(state, "prediction").get("status") != "done":
                state["prediction"] = {"status": "pending", "message": "Waiting for model — train one in Models tab", "ts": now}
        except Exception:
            pass

        # Mark done if all steps complete
        expected_steps = ["core_services", "infra_services", "buckets", "download", "upload",
                           "import_distances", "topics", "prediction"]
        if all(_is_done(state, s) for s in expected_steps):
            state["done"] = {"status": "done", "message": "Ready", "ts": now}
        else:
            state.pop("done", None)
    except Exception:
        pass
    return state

def _safe_get(state, key):
    v = state.get(key)
    return v if isinstance(v, dict) else {}

def _is_done(state, key):
    v = state.get(key)
    return isinstance(v, dict) and v.get("status") == "done"

@app.route("/api/pipeline/progress", methods=["POST"])
def api_pipeline_progress():
    data = request.get_json(silent=True) or {}
    step = data.get("step")
    status = data.get("status")
    message = data.get("message", "")
    if not step or not status:
        return json.dumps({"error": "step and status required"}), 400
    with _pipeline_state_lock:
        state = _load_pipeline_state()
        state[step] = {"status": status, "message": message, "ts": time.time()}
        state = _detect_pipeline_state(state)
        _save_pipeline_state(state)
    socketio.emit('pipeline_progress', {"step": step, "status": status, "message": message})
    return json.dumps({"ok": True})

@app.route("/api/pipeline/progress")
def api_get_pipeline_progress():
    with _pipeline_state_lock:
        state = _load_pipeline_state()
        state = _detect_pipeline_state(state)
        _save_pipeline_state(state)
    return json.dumps(state)

@app.route("/pipeline")
def pipeline_page():
    return render_template('pipeline.html')

_prediction_job_lock = threading.Lock()

def _restart_prediction_job():
    deploy_mode = os.getenv("DEPLOY_MODE", "")
    if deploy_mode == "gke":
        import requests as req, time
        with _prediction_job_lock:
            old_app_id = None
            try:
                r = req.get("http://spark-manager:8080/json/", timeout=3)
                for app in r.json().get("activeapps", []):
                    if "FlightDelayPrediction" in app.get("name", ""):
                        old_app_id = app.get("id", "")
            except Exception:
                pass
            try:
                jar = "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
                payload = {
                    "action": "CreateSubmissionRequest",
                    "appArgs": [],
                    "appResource": f"file:{jar}",
                    "clientSparkVersion": "4.1.1",
                    "environmentVariables": {},
                    "mainClass": "es.upm.dit.ging.predictor.MakePrediction",
                    "sparkProperties": {
                        "spark.master": "spark://spark-manager:7077",
                        "spark.submit.deployMode": "cluster",
                        "spark.cores.max": "2",
                        "spark.driver.memory": "2g",
                        "spark.executor.memory": "2g",
                        "spark.hadoop.fs.s3a.access.key": "admin",
                        "spark.hadoop.fs.s3a.secret.key": "password",
                        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                        "spark.hadoop.fs.s3a.path.style.access": "true",
                        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
                        "spark.sql.catalog.lakehouse.type": "hadoop",
                        "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
                        "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse",
                        "spark.sql.catalog.lakehouse.s3.endpoint": "http://minio:9000",
                        "spark.sql.catalog.lakehouse.s3.access-key": "admin",
                        "spark.sql.catalog.lakehouse.s3.secret-key": "password",
                        "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
                        "spark.sql.defaultCatalog": "lakehouse",
                        "spark.driverEnv.MODEL_VERSION": os.getenv("MODEL_VERSION", "1.0"),
                        "spark.driverEnv.BUCKETIZER_VERSION": os.getenv("BUCKETIZER_VERSION", "1.0"),
                    }
                }
                r = req.post("http://spark-manager:6066/v1/submissions/create", json=payload, timeout=10)
                if r.status_code in (200, 201):
                    print("[PRED] GKE prediction submitted via Spark REST API")
                    for _ in range(30):
                        time.sleep(2)
                        try:
                            rr = req.get("http://spark-manager:8080/json/", timeout=3)
                            new_active = [a for a in rr.json().get("activeapps", [])
                                          if "FlightDelayPrediction" in a.get("name", "")
                                          and a.get("id") != old_app_id]
                            if new_active:
                                print(f"[PRED] New prediction job {new_active[0]['id']} is active")
                                break
                        except Exception:
                            pass
                else:
                    print(f"[PRED] Spark REST submit failed: {r.status_code} {r.text[:200]}")
            except Exception as e:
                print(f"[PRED] GKE submit error: {e}")
            if old_app_id:
                try:
                    req.post(f"http://spark-manager:8080/app/kill/?id={old_app_id}&terminate=true", timeout=3)
                    print(f"[PRED] Killed old prediction job {old_app_id}")
                    time.sleep(5)
                except Exception:
                    pass
        return

    import docker, requests as req, threading, time

    with _prediction_job_lock:
        try:
            r = req.get("http://spark-manager:8080/json/", timeout=3)
            data = r.json()
            killed = 0
            # Kill stale prediction apps
            for app in data.get("activeapps", []):
                if "FlightDelayPrediction" in app.get("name", ""):
                    app_id = app.get("id", "")
                    req.post("http://spark-manager:8080/app/kill/", data={"id": app_id, "terminate": "true"}, timeout=3)
                    killed += 1
            # Kill stale prediction drivers
            for d in data.get("activedrivers", []):
                if "MakePrediction" in d.get("mainclass", "") and d.get("state") in ("RUNNING", "SUBMITTED"):
                    driver_id = d.get("id", "")
                    try:
                        req.post(f"http://spark-manager:6066/v1/submissions/kill/{driver_id}", timeout=3)
                    except Exception:
                        pass
                    killed += 1
            if killed > 0:
                print(f"[PRED] Killed {killed} stale prediction(s)")
                time.sleep(8)
        except Exception:
            pass

        try:
            access_key = os.getenv("MINIO_ROOT_USER", "admin")
            secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")
            prediction_jar = os.getenv("PREDICTION_JAR",
                "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar")
            cmd = (
                f"spark-submit --master spark://spark-manager:7077 "
                f"--deploy-mode cluster --conf spark.cores.max=2 "
                f"--conf spark.driver.memory=2g "
                f"--conf spark.executor.memory=2g "
                f"--conf spark.hadoop.fs.s3a.access.key={access_key} "
                f"--conf spark.hadoop.fs.s3a.secret.key={secret_key} "
                f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
                f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
                f"--conf spark.hadoop.fs.s3a.path.style.access=true "
                f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
                f"--conf spark.driverEnv.MODEL_VERSION={os.getenv('MODEL_VERSION', '1.0')} "
                f"--conf spark.driverEnv.BUCKETIZER_VERSION={os.getenv('BUCKETIZER_VERSION', '1.0')} "
                f"--class es.upm.dit.ging.predictor.MakePrediction "
                f"{prediction_jar}"
            )
            client = docker.from_env()
            container = client.containers.get("spark-manager")
            container.exec_run(cmd, environment={"MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")}, detach=True)
            print("[PRED] Prediction job submitted")
        except Exception as e:
            print(f"Submit prediction error: {e}")

@app.route("/api/prediction/restart", methods=["POST"])
def api_restart_prediction():
    _restart_prediction_job()
    return json.dumps({"ok": True})

@app.route("/api/prediction/status")
def api_prediction_status():
    import requests as req
    try:
        r = req.get("http://spark-manager:8080/json/", timeout=3)
        for app in r.json().get("activeapps", []):
            if "FlightDelayPrediction" in app.get("name", ""):
                return json.dumps({"running": True})
    except Exception:
        pass
    return json.dumps({"running": False})

_train_lock = threading.Lock()

@app.route("/api/models/train", methods=["POST"])
def api_train_model():
    import threading, docker, requests as req

    with _train_lock:
        if getattr(api_train_model, "_training", False):
            try:
                r = req.get("http://spark-manager:8080/json/", timeout=3)
                has_app = any("train_spark_mllib_model" in a.get("name", "")
                              for a in r.json().get("activeapps", []))
            except Exception:
                has_app = False
            if not has_app:
                api_train_model._training = False
            else:
                return json.dumps({"status": "already_running"}), 200

    # Read hyperparameters from request
    data = request.get_json(silent=True) or {}
    max_bins = data.get("max_bins", 4657)
    max_memory_mb = data.get("max_memory_mb", 1024)
    num_trees = data.get("num_trees", 20)
    max_depth = data.get("max_depth", 10)
    run_name = data.get("run_name", "").strip() or None

    if max_bins < 4200:
        return json.dumps({"error": f"maxBins ({max_bins}) too low. Dataset needs at least 4200. Slider range is 4200-10000."}), 400

    try:
        import time as _t

        with _train_lock:
            api_train_model._training = True
        _last_training["running"] = False
        _last_training["ts"] = _t.time()

        # Kill stale apps to free resources for training
        try:
            import requests as req
            r = req.get("http://spark-manager:8080/json/", timeout=3)
            jdata = r.json()
            for app in jdata.get("activeapps", []):
                if any(k in app.get("name", "") for k in ["train_spark_mllib_model", "FlightDelayPrediction"]):
                    app_id = app.get("id", "")
                    req.post("http://spark-manager:8080/app/kill/", data={"id": app_id, "terminate": "true"}, timeout=3)
                    print(f"[TRAIN] Killed {app.get('name')} ({app_id}) to free resources")
        except Exception:
            pass

        last_run_id = None
        try:
            import requests as req
            resp = req.post(f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/runs/search",
                json={"experiment_ids": ["0"], "order_by": ["start_time desc"], "max_results": 1, "run_view_type": "ACTIVE_ONLY"}, timeout=5)
            runs = resp.json().get("runs", [])
            if runs:
                last_run_id = runs[0]["info"]["run_id"]
        except Exception:
            pass

        def _train():
            try:
                deploy_mode = os.getenv("DEPLOY_MODE", "")
                jar = "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
                spark_conf = {
                    "spark.master": "spark://spark-manager:7077",
                    "spark.submit.deployMode": "cluster",
                    "spark.cores.max": "2",
                    "spark.driver.memory": "2g",
                    "spark.executor.memory": "2g",
                    "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER", "admin"),
                    "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD", "password"),
                    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.lakehouse.type": "hadoop",
                    "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
                    "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse",
                    "spark.sql.catalog.lakehouse.s3.endpoint": "http://minio:9000",
                    "spark.sql.catalog.lakehouse.s3.access-key": os.getenv("MINIO_ROOT_USER", "admin"),
                    "spark.sql.catalog.lakehouse.s3.secret-key": os.getenv("MINIO_ROOT_PASSWORD", "password"),
                    "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
                    "spark.sql.defaultCatalog": "lakehouse",
                    "spark.driver.extraJavaOptions": "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
                    "spark.driverEnv.MODEL_VERSION": os.getenv("MODEL_VERSION", "1.0"),
                    "spark.driverEnv.BUCKETIZER_VERSION": os.getenv("BUCKETIZER_VERSION", "1.0"),
                }
                app_args = [
                    "--max-bins", str(max_bins),
                    "--max-memory-mb", str(max_memory_mb),
                    "--num-trees", str(num_trees),
                    "--max-depth", str(max_depth),
                ]
                if run_name:
                    app_args.extend(["--run-name", run_name])

                if deploy_mode == "gke":
                    # Use Spark REST API (no Docker socket in GKE)
                    import requests as req
                    payload = {
                        "action": "CreateSubmissionRequest",
                        "appArgs": app_args,
                        "appResource": f"file:{jar}",
                        "clientSparkVersion": "4.1.1",
                        "environmentVariables": {
                            "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
                        },
                        "mainClass": "es.upm.dit.ging.predictor.TrainModel",
                        "sparkProperties": spark_conf,
                    }
                    r = req.post("http://spark-manager:6066/v1/submissions/create", json=payload, timeout=10)
                    if r.status_code not in (200, 201):
                        print(f"[TRAIN] Spark REST submit failed: {r.status_code} {r.text[:200]}")
                    else:
                        # Poll until job appears and finishes
                        import time as _t
                        for _ in range(120):
                            _t.sleep(5)
                            try:
                                rr = req.get("http://spark-manager:8080/json/", timeout=5)
                                data = rr.json()
                                active = [a for a in data.get("activeapps", [])
                                          if "train_spark_mllib_model" in a.get("name", "")
                                          and a.get("state") not in ("FINISHED", "KILLED", "FAILED")]
                                if not active:
                                    completed = [a for a in data.get("completedapps", [])
                                                 if "train_spark_mllib_model" in a.get("name", "")]
                                    if completed:
                                        print(f"[TRAIN] Job finished in GKE")
                                        break
                            except Exception:
                                pass
                else:
                    import docker as _dk
                    client = _dk.from_env()
                    container = client.containers.get("spark-manager")
                    cmd = "spark-submit --master spark://spark-manager:7077 --deploy-mode cluster "
                    for k, v in spark_conf.items():
                        cmd += f"--conf {k}={v} "
                    cmd += f"--class es.upm.dit.ging.predictor.TrainModel {jar} "
                    cmd += " ".join(app_args)
                    log_resp = container.exec_run(
                        cmd,
                        environment={
                            "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
                        },
                        detach=False,
                    )
                    if log_resp[0] != 0:
                        print(f"[TRAIN] Failed (exit {log_resp[0]}):\n{log_resp[1].decode('utf-8', errors='replace')[:2000]}")
            except Exception as e:
                print(f"Training error: {e}")
            finally:
                api_train_model._training = False
                _restart_prediction_job()

        t = threading.Thread(target=_train, daemon=True)
        t.start()
        return json.dumps({"status": "started", "last_run_id": last_run_id})
    except Exception as e:
        api_train_model._training = False
        print(f"Training setup error: {e}")
        return json.dumps({"error": str(e)}), 500

_last_training = {"running": False, "elapsed": 0, "ts": 0}

@app.route("/api/models/train/status")
def api_train_status():
    import requests as req, time as _time
    now = _time.time()
    is_training = getattr(api_train_model, "_training", False)
    try:
        r = req.get("http://spark-manager:8080/json/", timeout=5)
        data = r.json()
        # Check active apps first
        for app in data.get("activeapps", []):
            if "train_spark_mllib_model" in app.get("name", ""):
                state = app.get("state", "")
                if state in ("FINISHED", "KILLED", "FAILED"):
                    continue
                _last_training["running"] = True
                _last_training["elapsed"] = app["duration"] // 1000
                _last_training["ts"] = now
                return json.dumps({"status": "running", "elapsed": _last_training["elapsed"]})
        # Check completed apps — training just finished
        for app in data.get("completedapps", []):
            if "train_spark_mllib_model" in app.get("name", ""):
                if is_training:
                    api_train_model._training = False
                _last_training["running"] = False
                _last_training["elapsed"] = app.get("duration", 0) // 1000
                return json.dumps({"status": "completed", "elapsed": _last_training["elapsed"]})
        # No training app found — if _training flag is set, we're in the gap
        if is_training and now - _last_training.get("ts", 0) < 120:
            return json.dumps({"status": "running", "elapsed": _last_training.get("elapsed", 0)})
    except Exception:
        if is_training and now - _last_training.get("ts", 0) < 120:
            return json.dumps({"status": "running", "elapsed": _last_training.get("elapsed", 0)})
    _last_training["running"] = False
    return json.dumps({"status": "idle"})

@app.route("/api/models/train/cancel", methods=["POST"])
def api_cancel_training():
    import requests as req
    try:
        r = req.get("http://spark-manager:8080/json/", timeout=3)
        data = r.json()
        killed = []
        for app in data.get("activeapps", []):
            if "train_spark_mllib_model" in app.get("name", ""):
                app_id = app.get("id", "")
                req.post("http://spark-manager:8080/app/kill/", data={"id": app_id, "terminate": "true"}, timeout=3)
                killed.append(app_id)
        api_train_model._training = False
        return json.dumps({"ok": True, "killed": killed})
    except Exception as e:
        return json.dumps({"error": str(e)}), 500

@app.route("/api/models/delete/<run_id>", methods=["POST"])
def api_delete_model(run_id):
    import boto3
    from botocore.config import Config
    try:
        # 1. Delete from MLflow
        import requests as req
        mlflow_resp = req.post(f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/runs/delete",
            json={"run_id": run_id}, timeout=5)

        # 2. Delete artifacts from MinIO
        s3 = boto3.client("s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
            config=Config(signature_version="s3v4"))
        prefix = f"mlflow/0/{run_id}/"
        objs = s3.list_objects_v2(Bucket="lakehouse", Prefix=prefix)
        for obj in objs.get("Contents", []):
            s3.delete_object(Bucket="lakehouse", Key=obj["Key"])

        # 3. If this was the active model, reset active marker
        try:
            marker = s3.get_object(Bucket="lakehouse", Key="models/active_run_id.txt")
            active = marker["Body"].read().decode().strip()
            if active == run_id:
                s3.delete_object(Bucket="lakehouse", Key="models/active_run_id.txt")
        except Exception:
            pass

        return json.dumps({"ok": True})
    except Exception as e:
        return json.dumps({"error": str(e)}), 500

@app.route("/api/services/status")
def api_services_status():
    deploy_mode = os.getenv("DEPLOY_MODE", "")
    if deploy_mode == "gke":
        return json.dumps({"services": {}, "mode": "gke"})
    import docker
    expected = ['kafka', 'cassandra', 'spark-manager', 'spark-worker', 'flask', 'minio', 'mlflow', 'airflow-webserver', 'airflow-scheduler', 'airflow-postgres']
    services = {}
    try:
        client = docker.from_env()
        for name in expected:
            if name == 'flask':
                services[name] = {"status": "running", "image": "practica_creativa-flask"}
                continue
            try:
                c = client.containers.get(name)
                status = c.status
                services[name] = {"status": status, "image": c.image.tags[0] if c.image.tags else "—"}
            except docker.errors.NotFound:
                services[name] = {"status": "stopped", "image": "—"}
    except Exception as e:
        return json.dumps({"error": str(e), "services": {}})
    return json.dumps({"services": services})

@app.route("/api/gke/status")
def api_gke_status():
    """Get GKE cluster nodes and pods status using K8s in-cluster API."""
    if os.getenv("DEPLOY_MODE", "") != "gke":
        return json.dumps({"nodes": [], "pods": [], "error": "Only available in GKE mode"})
    result = {"nodes": [], "pods": [], "error": None}
    try:
        token = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read().strip()
        ca = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        host = os.getenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
        base = f"https://{host}:{port}"
        import subprocess
        def k8s_get(path):
            return subprocess.run(
                ["curl", "-s", "--cacert", ca, "-H", f"Authorization: Bearer {token}",
                 f"{base}{path}"],
                capture_output=True, text=True, timeout=10
            )
        r = k8s_get("/api/v1/nodes")
        if r.returncode == 0:
            import json
            nodes = json.loads(r.stdout).get("items", [])
            # Assign a stable color per node based on name hash
            node_colors = {}
            palette = [
                "#6366f1", "#ec4899", "#06b6d4", "#f59e0b",
                "#8b5cf6", "#84cc16", "#ef4444", "#3b82f6"
            ]
            for i, n in enumerate(nodes):
                short = n["metadata"]["name"].split("-")[-1][:5]
                node_colors[n["metadata"]["name"]] = palette[i % len(palette)]
                s = n.get("status", {})
                conditions = {c["type"]: c["status"] for c in s.get("conditions", [])}
                alloc = s.get("allocatable", s.get("capacity", {}))
                mem = alloc.get("memory", "?")
                if mem.endswith("Ki"): mem = f"{int(mem[:-2]) // (1024*1024)}Gi"
                result["nodes"].append({
                    "name": n["metadata"]["name"],
                    "short_id": short,
                    "color": palette[i % len(palette)],
                    "ready": conditions.get("Ready") == "True",
                    "cpu": alloc.get("cpu", "?"),
                    "memory": mem,
                    "instance_type": n["metadata"]["labels"].get("node.kubernetes.io/instance-type", "?"),
                })
        r2 = k8s_get("/api/v1/namespaces/ibdn/pods")
        if r2.returncode == 0:
            import json
            pods = json.loads(r2.stdout).get("items", [])
            for p in pods:
                ready = sum(1 for c in p.get("status", {}).get("containerStatuses", []) if c.get("ready"))
                total = len(p.get("status", {}).get("containerStatuses", []))
                start = p.get("status", {}).get("startTime", "?")
                node_name = p.get("spec", {}).get("nodeName", "")
                result["pods"].append({
                    "name": p["metadata"]["name"],
                    "ready": f"{ready}/{total}",
                    "status": p.get("status", {}).get("phase", "?"),
                    "restarts": sum(c.get("restartCount", 0) for c in p.get("status", {}).get("containerStatuses", [])),
                    "age": start[:19] if start else "?",
                    "node": node_name.split("-")[-1][:5] if node_name else "?",
                    "node_color": node_colors.get(node_name, "#555"),
                })
    except Exception as e:
        result["error"] = str(e)
    return json.dumps(result)

@app.route("/api/gke/scale", methods=["POST"])
def api_gke_scale():
    """Scale a GKE deployment or node pool using K8s API."""
    data = request.get_json(silent=True) or {}
    target = data.get("target", "")  # "deployment:name" or "nodes"
    replicas = data.get("replicas")
    try:
        token = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read().strip()
        ca = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        host = os.getenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
        base = f"https://{host}:{port}"
        import subprocess
        def k8s_api(method, path, body=None):
            cmd = ["curl", "-s", "--cacert", ca, "-H", f"Authorization: Bearer {token}",
                   "-X", method, f"{base}{path}"]
            if body:
                cmd += ["-H", "Content-Type: application/strategic-merge-patch+json",
                        "-d", body]
            return subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if target == "nodes":
            node_count = len(json.loads(k8s_api("GET", "/api/v1/nodes").stdout).get("items", []))
            if replicas is not None and replicas != node_count:
                import subprocess
                r = subprocess.run(
                    f"gcloud container clusters resize ibdn-cluster --node-pool default-pool "
                    f"--num-nodes {replicas} --zone {os.getenv('GCP_ZONE', 'europe-west1-b')} --project {os.getenv('GCP_PROJECT', 'REPLACE_PROJECT')} --quiet 2>&1",
                    shell=True, capture_output=True, text=True, timeout=60
                )
                cmd = (f"gcloud container clusters resize ibdn-cluster --node-pool default-pool "
                       f"--num-nodes {replicas} --zone {os.getenv('GCP_ZONE', 'europe-west1-b')} --project {os.getenv('GCP_PROJECT', 'REPLACE_PROJECT')} --quiet")
                if r.returncode == 0:
                    return json.dumps({"ok": True, "output": f"Node pool resizing to {replicas} nodes (from {node_count})"})
                err = (r.stdout.strip() + r.stderr.strip())[:300]
                return json.dumps({
                    "ok": False,
                    "command": cmd,
                    "output": f"Auto-resize unavailable.\n\nRun in your terminal:"
                })
            return json.dumps({"ok": True, "output": f"Already at {node_count} nodes"})
        else:
            deployment = data.get("deployment", target.replace("deployment:", ""))
            if not deployment or replicas is None:
                return json.dumps({"error": "deployment and replicas required"}), 400
            body = json.dumps({"spec": {"replicas": int(replicas)}})
            r = k8s_api("PATCH", f"/apis/apps/v1/namespaces/ibdn/deployments/{deployment}", body)
            return json.dumps({"ok": r.returncode == 0, "output": f"Scaled {deployment} to {replicas}"})
    except Exception as e:
        return json.dumps({"error": str(e)})

@app.route("/api/models/push-gke", methods=["POST"])
def api_models_push_gke():
    """Return command to push models from local to GKE MinIO."""
    deploy_mode = os.getenv("DEPLOY_MODE", "")
    if deploy_mode != "gke":
        return json.dumps({"error": "Only available in GKE mode"}), 400
    data = request.get_json(silent=True) or {}
    run_ids = data.get("run_ids", [])
    if not run_ids:
        return json.dumps({"error": "No models selected"}), 400
    ids_str = " ".join(run_ids)
    cmd = (
        f"# 1. Start MinIO port-forward:\n"
        f"kubectl port-forward -n ibdn deploy/minio 9001:9000 &\n\n"
        f"# 2. Push models (run from your local terminal):\n"
        f"predict models push-gke {ids_str}"
    )
    return json.dumps({"ok": False, "command": cmd, "output": "Push models from local to GKE:"})

@app.route("/api/models/push", methods=["POST"])
def api_models_push():
    """Push selected models to the GCloud VM."""
    import subprocess, tempfile, os, boto3
    from botocore.config import Config

    data = request.get_json(silent=True) or {}
    run_ids = data.get("run_ids", [])
    if not run_ids:
        return json.dumps({"error": "No models selected"}), 400

    gcp_project = os.getenv("GCP_PROJECT", "")
    gcp_zone = os.getenv("GCP_ZONE", "europe-west1-b")
    gcp_instance = os.getenv("GCP_INSTANCE", "bigdata-vm")
    gcp_user = os.getenv("GCP_USER", "ubuntu")
    gcp_repo = f"/home/{gcp_user}/ibdn"

    if not gcp_project:
        return json.dumps({"error": "GCP_PROJECT not configured"}), 500

    s3 = boto3.client("s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
        config=Config(signature_version="s3v4"))

    results = []
    for run_id in run_ids:
        try:
            prefix = f"mlflow/0/{run_id}/artifacts/model/"
            objs = s3.list_objects_v2(Bucket="lakehouse", Prefix=prefix)
            if not objs.get("Contents"):
                results.append({"run_id": run_id, "status": "no_model", "message": "No model artifacts"})
                continue

            with tempfile.TemporaryDirectory() as tmp:
                local_dir = os.path.join(tmp, "model")
                os.makedirs(local_dir)
                for obj in objs["Contents"]:
                    key = obj["Key"]
                    rel = key[len(prefix):]
                    if rel:
                        local_path = os.path.join(local_dir, rel)
                        os.makedirs(os.path.dirname(local_path), exist_ok=True)
                        s3.download_file("lakehouse", key, local_path)

                tar_name = f"model_{run_id[:12]}.tar.gz"
                tar_path = os.path.join(tmp, tar_name)
                subprocess.run(["tar", "-czf", tar_path, "-C", local_dir, "."], check=True)

                remote_path = f"{gcp_repo}/models_upload.tar.gz"
                subprocess.run([
                    "gcloud", "compute", "scp", tar_path,
                    f"{gcp_user}@{gcp_instance}:{remote_path}",
                    "--zone", gcp_zone, "--project", gcp_project, "--quiet"
                ], check=True)

                import_cmds = [
                    f"cd {gcp_repo} && tar -xzf models_upload.tar.gz -C /tmp/models_import_{run_id[:8]}",
                    f"docker cp /tmp/models_import_{run_id[:8]}/. minio:/tmp/models_import_{run_id[:8]}/",
                    f"docker exec minio mc cp --recursive /tmp/models_import_{run_id[:8]}/ local/lakehouse/models/",
                    f"rm -rf /tmp/models_import_{run_id[:8]} {gcp_repo}/models_upload.tar.gz",
                ]
                cmd_str = " && ".join(import_cmds)
                subprocess.run([
                    "gcloud", "compute", "ssh",
                    f"{gcp_user}@{gcp_instance}",
                    "--zone", gcp_zone, "--project", gcp_project,
                    "--quiet", "--command", cmd_str
                ], check=True)

                results.append({"run_id": run_id, "status": "pushed"})
        except Exception as e:
            results.append({"run_id": run_id, "status": "error", "message": str(e)})

    pushed = [r for r in results if r["status"] == "pushed"]
    return json.dumps({"results": results, "pushed": len(pushed)})

@app.route("/api/airflow/password")
def api_airflow_password():
    import subprocess
    deploy_mode = os.getenv("DEPLOY_MODE", "")
    if deploy_mode == "gke":
        try:
            token = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read().strip()
            ca = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
            host = os.getenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
            port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
            pods_json = subprocess.run(
                ["curl", "-s", "--cacert", ca, "-H", f"Authorization: Bearer {token}",
                 f"https://{host}:{port}/api/v1/namespaces/ibdn/pods?labelSelector=app=airflow-webserver&fieldSelector=status.phase=Running"],
                capture_output=True, text=True, timeout=10
            )
            items = json.loads(pods_json.stdout).get("items", [])
            if items:
                items.sort(key=lambda p: p["metadata"]["creationTimestamp"], reverse=True)
                r = subprocess.run(
                    ["curl", "-s", "--cacert", ca, "-H", f"Authorization: Bearer {token}",
                     f"https://{host}:{port}/api/v1/namespaces/ibdn/pods/{items[0]['metadata']['name']}/log?tailLines=5000&container=webserver"],
                    capture_output=True, text=True, timeout=10
                )
                for line in r.stdout.split('\n'):
                    if "Password for user admin:" in line:
                        pw = line.rsplit(": ", 1)[-1].strip().strip("'")
                        if pw:
                            return json.dumps({"username": "admin", "password": pw})
        except Exception:
            pass
        return json.dumps({"username": "admin", "password": None})
    try:
        r = subprocess.run(
            ["docker", "logs", "airflow-webserver"],
            capture_output=True, text=True, timeout=10
        )
        for line in (r.stderr + r.stdout).split('\n'):
            if "Password for user" in line:
                pw = line.rsplit(": ", 1)[-1].strip()
                if pw:
                    return json.dumps({"username": "admin", "password": pw})
    except Exception:
        pass
    return json.dumps({"username": "admin", "password": None})

import sys, os
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
from utils.logs import Logs

@app.route("/api/logs/all")
def api_all_logs():
    result = Logs.get_all_logs()
    return json.dumps(result)

@app.route("/api/logs/<service>")
def api_service_logs(service):
    tail = 2000 if service == 'flask' else request.args.get('tail', 500, type=int)
    result = Logs.get_service_logs(service, tail)
    if service == 'flask':
        result["logs"] = '\n'.join(l for l in result["logs"].split('\n') if '/api/logs/' not in l)
    if "error" in result:
        return json.dumps(result), 404 if "not found" in result["error"].lower() else 500
    return json.dumps(result)

# Setup Kafka
from kafka import KafkaProducer, KafkaConsumer

for _ in range(20):
  try:
    local_host, local_port = KAFKA_LOCAL_BOOTSTRAP_SERVERS.split(':')
    with socket.create_connection((local_host, int(local_port)), timeout=1):
      break
  except (ConnectionRefusedError, OSError, socket.timeout):
    time.sleep(1)

producer = None
PREDICTION_TOPIC = KAFKA_TOPIC

def get_producer():
    if not hasattr(get_producer, '_p'):
        get_producer._p = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                                         max_block_ms=10000)
    return get_producer._p

# Persist prediction based on DB_MODE
_PREDICTION_CACHE_FILE = '/tmp/prediction_cache.json'
import json as _json

def _save_prediction_to_cache(uuid, data):
  import os
  cache = {}
  if os.path.exists(_PREDICTION_CACHE_FILE):
    try:
      with open(_PREDICTION_CACHE_FILE) as f:
        cache = _json.load(f)
    except:
      cache = {}
  cache[uuid] = data
  # Keep only last 50
  if len(cache) > 50:
    cache = dict(list(cache.items())[-50:])
  with open(_PREDICTION_CACHE_FILE, 'w') as f:
    _json.dump(cache, f)

def _get_prediction_from_cache(uuid):
  import os
  if os.path.exists(_PREDICTION_CACHE_FILE):
    try:
      with open(_PREDICTION_CACHE_FILE) as f:
        cache = _json.load(f)
      return cache.get(uuid)
    except:
      pass
  return None

def persist_prediction(data):
  _save_prediction_to_cache(data.get("UUID", ""), data)
  session = predict_utils.get_cassandra_session()
  if session:
    session.execute("""
      INSERT INTO agile_data_science.flight_delay_ml_response (
        uuid, prediction, origin, dest, dep_delay, carrier,
        flight_date, flight_num, distance, route,
        day_of_year, day_of_month, day_of_week, timestamp
      ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
      data.get('UUID'), int(data.get('Prediction', 0)),
      data.get('Origin'), data.get('Dest'), float(data.get('DepDelay', 0.0)),
      data.get('Carrier'), data.get('FlightDate'), data.get('FlightNum'),
      data.get('Distance'), data.get('Route'),
      int(data.get('DayOfYear', 0)), int(data.get('DayOfMonth', 0)), int(data.get('DayOfWeek', 0)),
      str(data.get('Timestamp', ''))
    ))

@socketio.on('subscribe')
def on_subscribe(data):
    join_room(data['id'])

# Start Kafka consumers
def _kafka_response_listener():
  consumer = KafkaConsumer(
    KAFKA_RESPONSE_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset='earliest',
    group_id='flask-prediction-consumer'
  )
  for msg in consumer:
    try:
      data = json.loads(msg.value.decode('utf-8'))
      if isinstance(data, dict) and 'UUID' in data:
        print(f"Consumer processing UUID: {data['UUID'][:12]} ...")
        socketio.emit('spark_status', {'status': 'PROCESSING'}, room=data.get('UUID', ''))
        # Inline save to file cache  
        import json as _jj
        import os as _oo
        _cf = '/tmp/prediction_cache.json'
        _cd = {}
        if _oo.path.exists(_cf):
          try:
            with open(_cf) as _ff: _cd = _jj.load(_ff)
          except: _cd = {}
        _cd[data.get("UUID", "")] = data
        if len(_cd) > 50:
          _cd = dict(list(_cd.items())[-50:])
        with open(_cf, 'w') as _ff: _jj.dump(_cd, _ff)
        print(f"SAVED TO CACHE: {data.get('UUID','')[:12]}")
        session = predict_utils.get_cassandra_session()
        if session:
          session.execute("""
            INSERT INTO agile_data_science.flight_delay_ml_response (
              uuid, prediction, origin, dest, dep_delay, carrier,
              flight_date, flight_num, distance, route,
              day_of_year, day_of_month, day_of_week, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """, (
            data.get('UUID'), int(data.get('Prediction', 0)),
            data.get('Origin'), data.get('Dest'), float(data.get('DepDelay', 0.0)),
            data.get('Carrier'), data.get('FlightDate'), data.get('FlightNum'),
            data.get('Distance'), data.get('Route'),
            int(data.get('DayOfYear', 0)), int(data.get('DayOfMonth', 0)), int(data.get('DayOfWeek', 0)),
            str(data.get('Timestamp', ''))
          ))
        print(f"Consumer: saved event for {data['UUID'][:12]}")
        socketio.emit('saved', data, room=data.get('UUID', ''))
        socketio.emit('prediction', data, room=data.get('UUID', ''))
    except Exception as e:
      print(f"Consumer error: {e}")

def _kafka_status_listener():
  consumer = KafkaConsumer(
    KAFKA_STATUS_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset='latest'
  )
  for msg in consumer:
    try:
      status = msg.value.decode('utf-8')
      print(f"Status event: {status}")
      socketio.emit('spark_status', {'status': status})
    except Exception as e:
      print(f"Status error: {e}")

_kafka_response_listener_thread = threading.Thread(target=_kafka_response_listener, daemon=True)
_kafka_response_listener_thread.start()
_kafka_status_listener_thread = threading.Thread(target=_kafka_status_listener, daemon=True)
_kafka_status_listener_thread.start()

import uuid

# Chapter 5 controller: Fetch a flight and display it
@app.route("/on_time_performance")
def on_time_performance():
  
  carrier = request.args.get('Carrier')
  flight_date = request.args.get('FlightDate')
  flight_num = request.args.get('FlightNum')
  
  flight = _get_db().on_time_performance.find_one({
    'Carrier': carrier,
    'FlightDate': flight_date,
    'FlightNum': flight_num
  })
  
  return render_template('flight.html', flight=flight)

# Chapter 5 controller: Fetch all flights between cities on a given day and display them
@app.route("/flights/<origin>/<dest>/<flight_date>")
def list_flights(origin, dest, flight_date):
  
  flights = _get_db().on_time_performance.find(
    {
      'Origin': origin,
      'Dest': dest,
      'FlightDate': flight_date
    },
    sort = [
      ('DepTime', 1),
      ('ArrTime', 1),
    ]
  )
  flight_count = _get_db().on_time_performance.count_documents({
    'Origin': origin,
    'Dest': dest,
    'FlightDate': flight_date
  })
  
  return render_template(
    'flights.html',
    flights=flights,
    flight_date=flight_date,
    flight_count=flight_count
  )

# Controller: Fetch a flight table
@app.route("/total_flights")
def total_flights():
  total_flights = _get_db().flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return render_template('total_flights.html', total_flights=total_flights)

# Serve the chart's data via an asynchronous request (formerly known as 'AJAX')
@app.route("/total_flights.json")
def total_flights_json():
  total_flights = _get_db().flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return json.dumps(total_flights, ensure_ascii=False)

# Controller: Fetch a flight chart
@app.route("/total_flights_chart")
def total_flights_chart():
  total_flights = _get_db().flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return render_template('total_flights_chart.html', total_flights=total_flights)

@app.route("/airplanes")
@app.route("/airplanes/")
def search_airplanes():

  search_config = [
    {'field': 'TailNum', 'label': 'Tail Number'},
    {'field': 'Owner', 'sort_order': 0},
    {'field': 'OwnerState', 'label': 'Owner State'},
    {'field': 'Manufacturer', 'sort_order': 1},
    {'field': 'Model', 'sort_order': 2},
    {'field': 'ManufacturerYear', 'label': 'MFR Year'},
    {'field': 'SerialNumber', 'label': 'Serial Number'},
    {'field': 'EngineManufacturer', 'label': 'Engine MFR', 'sort_order': 3},
    {'field': 'EngineModel', 'label': 'Engine Model', 'sort_order': 4}
  ]

  # Pagination parameters
  start = request.args.get('start') or 0
  start = int(start)
  end = request.args.get('end') or config.AIRPLANE_RECORDS_PER_PAGE
  end = int(end)

  # Navigation path and offset setup
  nav_path = predict_utils.strip_place(request.url)
  nav_offsets = predict_utils.get_navigation_offsets(start, end, config.AIRPLANE_RECORDS_PER_PAGE)

  print("nav_path: [{}]".format(nav_path))
  print(json.dumps(nav_offsets))

  arg_dict = {}
  query = {}
  for item in search_config:
    field = item['field']
    value = request.args.get(field)
    print(field, value)
    arg_dict[field] = value
    if value:
      query[field] = value

  airplanes_cursor = _get_db().airplanes.find(query).sort('Owner', 1).skip(start).limit(end - start)
  airplanes = list(airplanes_cursor)
  airplane_count = _get_db().airplanes.count_documents(query)

  # Persist search parameters in the form template
  return render_template(
    'all_airplanes.html',
    search_config=search_config,
    args=arg_dict,
    airplanes=airplanes,
    airplane_count=airplane_count,
    nav_path=nav_path,
    nav_offsets=nav_offsets,
  )

@app.route("/airplanes/chart/manufacturers.json")
@app.route("/airplanes/chart/manufacturers.json")
def airplane_manufacturers_chart():
  mfr_chart = _get_db().airplane_manufacturer_totals.find_one()
  return json.dumps(mfr_chart)

# Controller: Fetch a flight and display it
@app.route("/airplane/<tail_number>")
@app.route("/airplane/flights/<tail_number>")
def flights_per_airplane(tail_number):
  flights = _get_db().flights_per_airplane.find_one(
    {'TailNum': tail_number}
  )
  return render_template(
    'flights_per_airplane.html',
    flights=flights,
    tail_number=tail_number
  )

# Controller: Fetch an airplane entity page
@app.route("/airline/<carrier_code>")
def airline(carrier_code):
  airline_summary = _get_db().airlines.find_one(
    {'CarrierCode': carrier_code}
  )
  airline_airplanes = _get_db().airplanes_per_carrier.find_one(
    {'Carrier': carrier_code}
  )
  return render_template(
    'airlines.html',
    airline_summary=airline_summary,
    airline_airplanes=airline_airplanes,
    carrier_code=carrier_code
  )

# Home page — flight delay prediction
@app.route("/")
def index():
    state = _load_pipeline_state()
    if not state.get("done"):
        import requests as req
        try:
            r = req.get("http://spark-manager:8080/json/", timeout=3)
            has_prediction = any("FlightDelayPrediction" in a.get("name", "")
                                for a in r.json().get("activeapps", []))
        except Exception:
            has_prediction = False
        if not has_prediction:
            return render_template('pipeline.html')
    _save_pipeline_state({"done": True, "steps": []})
    form_config = [
        {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5, 'type': 'number'},
        {'field': 'Carrier', 'value': 'AA', 'type': 'text'},
        {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25', 'type': 'date'},
        {'field': 'Origin', 'value': 'ATL', 'type': 'text'},
        {'field': 'Dest', 'label': 'Destination', 'value': 'SFO', 'type': 'text'},
    ]
    return render_template('flight_delays_predict_kafka.html', form_config=form_config)

@app.route("/airlines")
@app.route("/airlines/")
def airlines():
  airlines = _get_db().airplanes_per_carrier.find()
  return render_template('all_airlines.html', airlines=airlines)

@app.route("/flights/search")
@app.route("/flights/search/")
def search_flights():

  # Search parameters
  carrier = request.args.get('Carrier')
  flight_date = request.args.get('FlightDate')
  origin = request.args.get('Origin')
  dest = request.args.get('Dest')
  tail_number = request.args.get('TailNum')
  flight_number = request.args.get('FlightNum')

  # Pagination parameters
  start = request.args.get('start') or 0
  start = int(start)
  end = request.args.get('end') or config.RECORDS_PER_PAGE
  end = int(end)

  # Navigation path and offset setup
  nav_path = predict_utils.strip_place(request.url)
  nav_offsets = predict_utils.get_navigation_offsets(start, end, config.RECORDS_PER_PAGE)

  query = {}
  if carrier:
    query['Carrier'] = carrier
  if flight_date:
    query['FlightDate'] = flight_date
  if origin:
    query['Origin'] = origin
  if dest:
    query['Dest'] = dest
  if tail_number:
    query['TailNum'] = tail_number
  if flight_number:
    query['FlightNum'] = flight_number

  flights_cursor = _get_db().on_time_performance.find(query).sort([
    ('FlightDate', 1),
    ('DepTime', 1),
    ('Carrier', 1),
    ('FlightNum', 1)
  ]).skip(start).limit(end - start)
  flights = list(flights_cursor)
  flight_count = _get_db().on_time_performance.count_documents(query)

  # Persist search parameters in the form template
  return render_template(
    'search.html',
    flights=flights,
    flight_date=flight_date,
    flight_count=flight_count,
    nav_path=nav_path,
    nav_offsets=nav_offsets,
    carrier=carrier,
    origin=origin,
    dest=dest,
    tail_number=tail_number,
    flight_number=flight_number
    )

@app.route("/delays")
def delays():
  return render_template('delays.html')

# Load our regression model
import joblib
from os import environ


project_home = os.environ["PROJECT_HOME"]
# vectorizer = joblib.load("{}/models/sklearn_vectorizer.pkl".format(project_home))
# regressor = joblib.load("{}/models/sklearn_regressor.pkl".format(project_home))

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/regress", methods=['POST'])
def regress_flight_delays():
  
  api_field_type_map = \
    {
      "DepDelay": int,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }
  
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values
  prediction_features = {}
  prediction_features['Origin'] = api_form_values['Origin']
  prediction_features['Dest'] = api_form_values['Dest']
  prediction_features['FlightNum'] = api_form_values['FlightNum']
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(api_form_values['Origin'], api_form_values['Dest'])
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(api_form_values['FlightDate'])
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Vectorize the features
  feature_vectors = vectorizer.transform([prediction_features])
  
  # Make the prediction!
  result = regressor.predict(feature_vectors)[0]
  
  # Return a JSON object
  result_obj = {"Delay": result}
  return json.dumps(result_obj)

@app.route("/flights/delays/predict")
def flight_delays_page():
  """Serves flight delay predictions"""
  
  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'},
    {'field': 'FlightNum', 'label': 'Flight Number', 'value': 1519},
  ]
  
  return render_template('flight_delays_predict.html', form_config=form_config)

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/classify", methods=['POST'])
def classify_flight_delays():
  """POST API for classifying flight delays"""
  api_field_type_map = \
    {
      "DepDelay": float,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }
  
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values, which excludes Date
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(
    api_form_values['Origin'], api_form_values['Dest']
  )
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(
    api_form_values['FlightDate']
  )
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Add a timestamp
  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
  
  _get_db().prediction_tasks.insert_one(
    prediction_features
  )
  return json.dumps(prediction_features)

@app.route("/flights/delays/predict_batch")
def flight_delays_batch_page():
  """Serves flight delay predictions"""
  
  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'},
    {'field': 'FlightNum', 'label': 'Flight Number', 'value': 1519},
  ]
  
  return render_template("flight_delays_predict_batch.html", form_config=form_config)

@app.route("/flights/delays/predict_batch/results/<iso_date>")
def flight_delays_batch_results_page(iso_date):
  """Serves page for batch prediction results"""
  
  # Get today and tomorrow's dates as iso strings to scope query
  today_dt = iso8601.parse_date(iso_date)
  rounded_today = today_dt.date()
  iso_today = rounded_today.isoformat()
  rounded_tomorrow_dt = rounded_today + datetime.timedelta(days=1)
  iso_tomorrow = rounded_tomorrow_dt.isoformat()
  
  # Fetch today's prediction results
  predictions = _get_db().prediction_results.find(
    {
      'Timestamp': {
        "$gte": iso_today,
        "$lte": iso_tomorrow,
      }
    }
  )
  
  return render_template(
    "flight_delays_predict_batch_results.html",
    predictions=predictions,
    iso_date=iso_date
  )

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
  """POST API for classifying flight delays"""
  
  # Define the form fields to process
  api_field_type_map = \
    {
      "DepDelay": float,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }

  # Fetch the values for each field from the form object
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values, which excludes Date
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(
    api_form_values['Origin'], api_form_values['Dest']
  )
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(
    api_form_values['FlightDate']
  )
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Add a timestamp
  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
  
  # Create a unique ID for this message
  unique_id = str(uuid.uuid4())
  prediction_features['UUID'] = unique_id
  
  message_bytes = json.dumps(prediction_features).encode()
  p = get_producer()
  try:
    future = p.send(PREDICTION_TOPIC, message_bytes)
    future.add_callback(
      lambda x: socketio.emit('kafka_ack', {'id': unique_id}, room=unique_id)
    )
    future.add_errback(
      lambda x: print(f"Kafka send failed: {x}")
    )
    p.flush(timeout=10)
    print(f"Prediction sent: {unique_id[:12]} to {PREDICTION_TOPIC}")
  except Exception as e:
    print(f"Kafka send error: {e}")
    return json.dumps({"status": "ERROR", "error": f"Kafka send failed: {e}"}), 503

  return json.dumps({"status": "OK", "id": unique_id})

@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
  """Serves flight delay prediction page with polling form"""
  
  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'}
  ]
  
  return render_template('flight_delays_predict_kafka.html', form_config=form_config)

@app.route("/flights/delays/predict/classify_realtime/response/<unique_id>")
def classify_flight_delays_realtime_response(unique_id):
  """Serves predictions to polling requestors"""
  db_mode = os.getenv('DB_MODE', 'cassandra')

  if db_mode == 'cassandra':
    session = predict_utils.get_cassandra_session()
    if session:
      row = session.execute(
        "SELECT * FROM flight_delay_ml_response WHERE uuid=%s",
        (unique_id,)
      ).one()
      if row:
        return json.dumps({
          "status": "OK", "id": unique_id,
          "prediction": {
            "UUID": row.uuid, "Prediction": row.prediction,
            "Origin": row.origin, "Dest": row.dest,
            "DepDelay": row.dep_delay, "Carrier": row.carrier,
            "FlightDate": row.flight_date, "FlightNum": row.flight_num,
            "Distance": row.distance, "Route": row.route,
            "DayOfYear": row.day_of_year, "DayOfMonth": row.day_of_month,
            "DayOfWeek": row.day_of_week, "Timestamp": str(row.timestamp),
          }
        })

  else:
    prediction = _get_db().flight_delay_ml_response.find_one({"UUID": unique_id})
    if prediction:
      return json.dumps({"status": "OK", "id": unique_id, "prediction": prediction})

  cached = _get_prediction_from_cache(unique_id)
  if cached:
    return json.dumps({"status": "OK", "id": unique_id, "prediction": cached})
  return json.dumps({"status": "WAIT", "id": unique_id})

def shutdown_server():
  func = request.environ.get('werkzeug.server.shutdown')
  if func is None:
    raise RuntimeError('Not running with the Werkzeug Server')
  func()

@app.route('/shutdown')
def shutdown():
  shutdown_server()
  return 'Server shutting down...'

MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT') or 'http://minio:9000'
MODEL_FIXED_PATH = 's3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin'
MLFLOW_ARTIFACT_ROOT = 's3a://lakehouse/mlflow'

def _mlflow_request(method, path, data=None):
  import requests
  url = f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/{path}"
  r = requests.request(method, url, json=data, timeout=10)
  return r.json() if r.status_code == 200 else {}

@app.route("/models")
def models_page():
  import requests as req
  MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

  run_id_to_registry = {}
  try:
      resp = req.post(f"{MLFLOW_URI}/api/2.0/mlflow/registered-models/search",
          json={"max_results": 100}, timeout=5)
      for rm in resp.json().get("registered_models", []):
          for mv in rm.get("latest_versions", []):
              run_id_to_registry[mv["run_id"]] = {
                  "name": rm["name"],
                  "version": mv["version"],
                  "stage": mv.get("current_stage", "None"),
              }
  except Exception:
      pass

  runs = _mlflow_request("POST", "runs/search", {"experiment_ids": ["0"], "order_by": ["start_time desc"]})
  model_list = []
  for run in runs.get("runs", []):
    info = run["info"]
    data = run["data"]
    run_id = info["run_id"]
    params = {p["key"]: p["value"] for p in data.get("params", [])}
    metrics = {m["key"]: m["value"] for m in data.get("metrics", [])}
    reg = run_id_to_registry.get(run_id, {})
    model_list.append({
      "run_id": run_id,
      "run_name": info.get("run_name", "—"),
      "status": info["status"],
      "start_time": info["start_time"],
      "params": params,
      "metrics": metrics,
      "stage": reg.get("stage", "None"),
      "version": reg.get("version"),
      "registered_name": reg.get("name"),
    })

  active_run_id = None
  for m in model_list:
      if m.get("stage") == "Production":
          active_run_id = m["run_id"]
          break

  if not active_run_id:
      try:
        import boto3
        from botocore.config import Config
        s3 = boto3.client("s3",
          endpoint_url=MINIO_ENDPOINT,
          aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
          aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
          config=Config(signature_version="s3v4"))
        marker = s3.get_object(Bucket="lakehouse", Key="models/active_run_id.txt")
        active_run_id = marker["Body"].read().decode().strip()
      except Exception:
        active_run_id = None

  return render_template("models.html",
    models=model_list,
    active_run_id=active_run_id,
  )

@app.route("/models/activate/<run_id>")
def activate_model(run_id):
  import requests as req
  MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
  MODEL_NAME = "FlightDelayRF"

  run = _mlflow_request("GET", f"runs/get?run_id={run_id}")
  run_info = run.get("run", {})
  if not run_info:
    return "Run not found", 404

  try:
    import subprocess
    marker_path = "local/lakehouse/models/active_run_id.txt"
    subprocess.run(
        ["docker", "exec", "minio", "mc", "rm", "--recursive", "--force", marker_path],
        capture_output=True
    )
    subprocess.run(
        ["docker", "exec", "-i", "minio", "mc", "pipe", marker_path],
        input=run_id.encode(), check=True, capture_output=True
    )
  except Exception as e:
    return f"Error writing active marker: {e}", 500

  try:
      resp = req.post(f"{MLFLOW_URI}/api/2.0/mlflow/registered-models/search",
          json={"max_results": 100}, timeout=5)
      version = None
      rm_name = MODEL_NAME
      for rm in resp.json().get("registered_models", []):
          rm_name = rm["name"]
          for mv in rm.get("latest_versions", []):
              if mv.get("run_id") == run_id:
                  version = mv["version"]
                  break
      if version is None:
          register_resp = req.post(f"{MLFLOW_URI}/api/2.0/mlflow/model-versions/create",
              json={"name": MODEL_NAME, "source": f"runs:/{run_id}/model"}, timeout=5)
          if register_resp.status_code == 200:
              version = register_resp.json().get("model_version", {}).get("version")
      if version:
          req.post(f"{MLFLOW_URI}/api/2.0/mlflow/model-versions/transition-stage",
              json={"name": rm_name, "version": str(version), "stage": "Production"}, timeout=5)
  except Exception:
      pass

  _restart_prediction_job()
  return redirect(url_for("models_page"))

def _auto_start_prediction():
    """Start prediction job on Flask boot if there's an active model and no running job."""
    if os.getenv('SKIP_AUTO_START_PREDICTION'):
        print("[BOOT] SKIP_AUTO_START_PREDICTION set, skipping auto-start")
        return
    import boto3
    from botocore.config import Config
    def _boot_start():
        time.sleep(5)  # Wait for Kafka to be ready
        try:
            import requests as req
            r = req.get("http://spark-manager:8080/json/", timeout=3)
            has_pred = any("FlightDelayPrediction" in a.get("name", "") for a in r.json().get("activeapps", []))
            if has_pred:
                print("[BOOT] Prediction job already running, skipping auto-start")
                return
        except Exception:
            pass
        try:
            s3 = boto3.client("s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
                config=Config(signature_version="s3v4"))
            marker = s3.get_object(Bucket="lakehouse", Key="models/active_run_id.txt")
            active = marker["Body"].read().decode().strip()
            if active:
                print(f"[BOOT] Active model found ({active[:12]}...), starting prediction job")
                _restart_prediction_job()
            else:
                print("[BOOT] No active model, skipping prediction auto-start")
        except Exception:
            print("[BOOT] No active model found, skipping prediction auto-start")
    threading.Thread(target=_boot_start, daemon=True).start()

_auto_start_prediction()

if __name__ == "__main__":
    socketio.run(
    app,
    debug=True,
    use_reloader=False,
    host='0.0.0.0',
    port=int(os.getenv('FLASK_PORT', '5001')),
    allow_unsafe_werkzeug=True
  )
