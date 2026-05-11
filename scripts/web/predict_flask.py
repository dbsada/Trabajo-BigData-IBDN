import sys, os, re
from flask import Flask, render_template, request, redirect, url_for
from pymongo import MongoClient
from bson import json_util
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

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://mongodb:27017/')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'agile_data_science')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_LOCAL_BOOTSTRAP_SERVERS = os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'flight-delay-ml-request')
KAFKA_RESPONSE_TOPIC = os.getenv('KAFKA_RESPONSE_TOPIC', 'flight-delay-ml-response')
KAFKA_STATUS_TOPIC = os.getenv('KAFKA_STATUS_TOPIC', 'flight-delay-ml-status')

client = MongoClient(MONGODB_URI)
db = client[MONGODB_DATABASE]

@app.context_processor
def inject_arch_info():
    host = None
    try:
        host = __import__('subprocess').run(
            ['hostname', '-I'], capture_output=True, text=True
        ).stdout.strip().split()[0]
    except:
        pass
    return dict(
        VM_IP=host or 'localhost',
        DB_MODE=os.getenv('DB_MODE', 'cassandra'),
        KAFKA_TOPIC=os.getenv('KAFKA_TOPIC', 'flight-delay-ml-request'),
        KAFKA_RESPONSE_TOPIC=os.getenv('KAFKA_RESPONSE_TOPIC', 'flight-delay-ml-response'),
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
    return json_util.dumps(match[:50])

@app.route("/api/models")
def api_models():
    import requests
    import boto3
    from botocore.config import Config

    # Fetch runs from MLflow
    try:
        resp = requests.post(f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/runs/search",
            json={"experiment_ids": ["0"], "order_by": ["start_time desc"]}, timeout=5)
        runs_data = resp.json().get("runs", [])
        runs_data = [r for r in runs_data if r.get("info", {}).get("lifecycle_stage", "active") != "deleted"
            and r.get("info", {}).get("run_name") != "Original"]
    except Exception:
        runs_data = []

    model_list = []
    model_list.append({
        "run_id": "builtin",
        "run_name": "Original",
        "status": "READY",
        "start_time": 0,
        "params": {"maxBins": "4657", "maxMemoryInMB": "1024", "numTrees": "20", "maxDepth": "10"},
        "metrics": {"accuracy": 0.588},
        "builtin": True,
    })
    for run in runs_data:
        info = run["info"]
        data = run["data"]
        params = {p["key"]: p["value"] for p in data.get("params", [])}
        metrics = {m["key"]: m["value"] for m in data.get("metrics", [])}
        model_list.append({
            "run_id": info["run_id"],
            "run_name": info.get("run_name", info["run_id"][:8]),
            "status": info["status"],
            "start_time": info["start_time"],
            "params": params,
            "metrics": metrics,
        })

    # Read active marker
    active_run_id = None
    try:
        s3 = boto3.client("s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
            config=Config(signature_version="s3v4"))
        marker = s3.get_object(Bucket="lakehouse", Key="models/active_run_id.txt")
        active_run_id = marker["Body"].read().decode().strip()
    except Exception:
        active_run_id = "builtin"

    return json_util.dumps({"models": model_list, "active_run_id": active_run_id})

@app.route("/api/models/activate/<run_id>", methods=["POST"])
def api_activate_model(run_id):
    import boto3, os
    from botocore.config import Config

    try:
        s3 = boto3.client("s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
            config=Config(signature_version="s3v4"))

        dst_prefix = "models/spark_random_forest_classifier.flight_delays.5.0.bin/"

        if run_id == "builtin":
            import requests as req
            # Find the MLflow run named "Original"
            resp = req.post(f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/runs/search",
                json={"experiment_ids": ["0"], "order_by": ["start_time desc"],
                      "filter": "tags.mlflow.runName = 'Original'"}, timeout=5)
            runs = resp.json().get("runs", [])
            if not runs:
                return json_util.dumps({"error": "Original model not found in MLflow. Run: docker exec spark spark-submit scripts/register_original.py"}), 404
            original_run_id = runs[0]["info"]["run_id"]
            src_prefix = f"mlflow/0/{original_run_id}/artifacts/model/"
            stages = s3.list_objects_v2(Bucket="lakehouse", Prefix=f"{src_prefix}sparkml/stages/", Delimiter="/")
            stage_prefix = None
            for p in stages.get("CommonPrefixes", []):
                stage_prefix = p["Prefix"]
                break
            if not stage_prefix:
                return json_util.dumps({"error": "Original model files not found in MLflow"}), 404
            existing = s3.list_objects_v2(Bucket="lakehouse", Prefix=dst_prefix)
            for obj in existing.get("Contents", []):
                s3.delete_object(Bucket="lakehouse", Key=obj["Key"])
            objects = s3.list_objects_v2(Bucket="lakehouse", Prefix=stage_prefix)
            for obj in objects.get("Contents", []):
                rel_path = obj["Key"][len(stage_prefix):]
                s3.copy_object(Bucket="lakehouse",
                    CopySource={"Bucket": "lakehouse", "Key": obj["Key"]},
                    Key=f"{dst_prefix}{rel_path}")
        else:
            src_prefix = f"mlflow/0/{run_id}/artifacts/model/"
            stages = s3.list_objects_v2(Bucket="lakehouse", Prefix=f"{src_prefix}sparkml/stages/", Delimiter="/")
            stage_prefix = None
            for p in stages.get("CommonPrefixes", []):
                stage_prefix = p["Prefix"]
                break
            if not stage_prefix:
                return json_util.dumps({"error": "No model stages found"}), 400

            existing = s3.list_objects_v2(Bucket="lakehouse", Prefix=dst_prefix)
            for obj in existing.get("Contents", []):
                s3.delete_object(Bucket="lakehouse", Key=obj["Key"])

            objects = s3.list_objects_v2(Bucket="lakehouse", Prefix=stage_prefix)
            for obj in objects.get("Contents", []):
                rel_path = obj["Key"][len(stage_prefix):]
                s3.copy_object(Bucket="lakehouse",
                    CopySource={"Bucket": "lakehouse", "Key": obj["Key"]},
                    Key=f"{dst_prefix}{rel_path}")

        s3.put_object(Bucket="lakehouse", Key="models/active_run_id.txt", Body=run_id.encode())
        return json_util.dumps({"ok": True, "active_run_id": run_id})
    except Exception as e:
        return json_util.dumps({"error": str(e)}), 500

def _restart_prediction_job():
    import docker, requests as req, threading
    try:
        # Kill existing prediction apps
        r = req.get("http://spark:8080/json/", timeout=3)
        data = r.json()
        for app in data.get("activeapps", []):
            if "FlightDelayPrediction" in app.get("name", ""):
                req.post("http://spark:8080/app/kill/", data={"id": app.get("id",""), "terminate": "true"}, timeout=3)

        # Wait a moment then resubmit
        def _submit():
            import time
            time.sleep(5)
            client = docker.from_env()
            access_key = os.getenv("MINIO_ROOT_USER", "admin")
            secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")
            prediction_jar = os.getenv("PREDICTION_JAR",
                "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar")
            cmd = (
                f"spark-submit --master spark://spark:7077 "
                f"--deploy-mode cluster --conf spark.cores.max=2 "
                f"--conf spark.hadoop.fs.s3a.access.key={access_key} "
                f"--conf spark.hadoop.fs.s3a.secret.key={secret_key} "
                f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
                f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
                f"--conf spark.hadoop.fs.s3a.path.style.access=true "
                f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
                f"--class es.upm.dit.ging.predictor.MakePrediction "
                f"{prediction_jar}"
            )
            container = client.containers.get("spark")
            container.exec_run(cmd, environment={"MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")}, detach=True)

        threading.Thread(target=_submit, daemon=True).start()
    except Exception as e:
        print(f"Restart prediction error: {e}")

@app.route("/api/prediction/restart", methods=["POST"])
def api_restart_prediction():
    _restart_prediction_job()
    return json_util.dumps({"ok": True})

@app.route("/api/models/train", methods=["POST"])
def api_train_model():
    import threading, docker
    if getattr(api_train_model, "_training", False):
        return json_util.dumps({"status": "already_running"}), 200

    # Read hyperparameters from request
    data = request.get_json(silent=True) or {}
    max_bins = data.get("max_bins", 4657)
    max_memory_mb = data.get("max_memory_mb", 1024)
    num_trees = data.get("num_trees", 20)
    max_depth = data.get("max_depth", 10)

    try:
        client = docker.from_env()

        api_train_model._training = True

        # Kill any stale training apps or resource-hogging apps on Spark
        try:
            import requests as req
            r = req.get("http://spark:8080/json/", timeout=3)
            data = r.json()
            for app in data.get("activeapps", []):
                if "train_spark_mllib_model" in app.get("name", ""):
                    app_id = app.get("id", "")
                    req.post("http://spark:8080/app/kill/", data={"id": app_id, "terminate": "true"}, timeout=3)
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
                container = client.containers.get("spark")
                container.exec_run(
                    "spark-submit --master spark://spark:7077 "
                    "--conf spark.cores.max=1 "
                    "--conf spark.hadoop.fs.s3a.access.key=" + os.getenv("MINIO_ROOT_USER", "admin") + " "
                    "--conf spark.hadoop.fs.s3a.secret.key=" + os.getenv("MINIO_ROOT_PASSWORD", "password") + " "
                    "scripts/train.py "
                    "--max-bins " + str(max_bins) + " "
                    "--max-memory-mb " + str(max_memory_mb) + " "
                    "--num-trees " + str(num_trees) + " "
                    "--max-depth " + str(max_depth) + " ",
                    environment={
                        "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
                    },
                    detach=False,
                )
            except Exception as e:
                print(f"Training error: {e}")
            finally:
                api_train_model._training = False
                import threading
                threading.Thread(target=_restart_prediction_job, daemon=True).start()

        t = threading.Thread(target=_train, daemon=True)
        t.start()
        return json_util.dumps({"status": "started", "last_run_id": last_run_id})
    except Exception as e:
        return json_util.dumps({"error": str(e)}), 500

@app.route("/api/models/train/status")
def api_train_status():
    import requests as req
    try:
        r = req.get("http://spark:8080/json/", timeout=3)
        data = r.json()
        for app in data.get("activeapps", []):
            if "train_spark_mllib_model" in app.get("name", ""):
                return json_util.dumps({"status": "running", "elapsed": app["duration"] // 1000})
    except Exception:
        pass
    if getattr(api_train_model, "_training", False):
        return json_util.dumps({"status": "running", "elapsed": 0})
    return json_util.dumps({"status": "idle"})

@app.route("/api/models/train/cancel", methods=["POST"])
def api_cancel_training():
    import requests as req
    try:
        r = req.get("http://spark:8080/json/", timeout=3)
        data = r.json()
        killed = []
        for app in data.get("activeapps", []):
            if "train_spark_mllib_model" in app.get("name", ""):
                app_id = app.get("id", "")
                req.post("http://spark:8080/app/kill/", data={"id": app_id, "terminate": "true"}, timeout=3)
                killed.append(app_id)
        api_train_model._training = False
        return json_util.dumps({"ok": True, "killed": killed})
    except Exception as e:
        return json_util.dumps({"error": str(e)}), 500

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

        return json_util.dumps({"ok": True})
    except Exception as e:
        return json_util.dumps({"error": str(e)}), 500

@app.route("/api/services/status")
def api_services_status():
    import docker
    expected = ['kafka', 'mongodb', 'cassandra', 'spark', 'spark-worker', 'flask', 'minio', 'mlflow']
    services = {}
    db_mode = os.getenv('DB_MODE', 'cassandra')
    try:
        client = docker.from_env()
        for name in expected:
            try:
                c = client.containers.get(name)
                status = c.status
                if status == 'running':
                    if name == db_mode:
                        status = 'active'
                    elif name in ('mongodb', 'cassandra'):
                        status = 'inactive'
                services[name] = {"status": status, "image": c.image.tags[0] if c.image.tags else "—"}
            except docker.errors.NotFound:
                if name in ('mongodb', 'cassandra'):
                    services[name] = {"status": "inactive", "image": "—"}
                else:
                    services[name] = {"status": "stopped", "image": "—"}
    except Exception as e:
        return json_util.dumps({"error": str(e), "services": {}})
    return json_util.dumps({"services": services})

# Setup Kafka
from kafka import KafkaProducer, KafkaConsumer

for _ in range(20):
  try:
    local_host, local_port = KAFKA_LOCAL_BOOTSTRAP_SERVERS.split(':')
    with socket.create_connection((local_host, int(local_port)), timeout=1):
      break
  except (ConnectionRefusedError, socket.timeout):
    time.sleep(1)

producer = None
PREDICTION_TOPIC = KAFKA_TOPIC

def get_producer():
    if not hasattr(get_producer, '_p'):
        get_producer._p = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                                         max_block_ms=10000)
    return get_producer._p

# Persist prediction based on DB_MODE
def persist_prediction(data):
  db_mode = os.getenv('DB_MODE', 'cassandra')
  if db_mode == 'cassandra':
    session = predict_utils.get_cassandra_session()
    if session:
      session.execute("""
        CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (
          uuid text PRIMARY KEY,
          prediction int,
          origin text, dest text, dep_delay double,
          carrier text, flight_date text, flight_num text,
          distance double, route text,
          day_of_year int, day_of_month int, day_of_week int,
          timestamp text
        )
      """)
      session.execute("""
        INSERT INTO agile_data_science.flight_delay_ml_response 
        (uuid, prediction, origin, dest, dep_delay, carrier, flight_date, flight_num,
         distance, route, day_of_year, day_of_month, day_of_week, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      """, (
        data.get('UUID'), int(data.get('Prediction', 0)),
        data.get('Origin'), data.get('Dest'), data.get('DepDelay'),
        data.get('Carrier'), data.get('FlightDate'), data.get('FlightNum'),
        data.get('Distance'), data.get('Route'),
        int(data.get('DayOfYear', 0)), int(data.get('DayOfMonth', 0)), int(data.get('DayOfWeek', 0)),
        str(data.get('Timestamp', ''))
      ))
    return

  db.flight_delay_ml_response.insert_one(data)

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
        persist_prediction(data)
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
  
  flight = db.on_time_performance.find_one({
    'Carrier': carrier,
    'FlightDate': flight_date,
    'FlightNum': flight_num
  })
  
  return render_template('flight.html', flight=flight)

# Chapter 5 controller: Fetch all flights between cities on a given day and display them
@app.route("/flights/<origin>/<dest>/<flight_date>")
def list_flights(origin, dest, flight_date):
  
  flights = db.on_time_performance.find(
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
  flight_count = db.on_time_performance.count_documents({
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
  total_flights = db.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return render_template('total_flights.html', total_flights=total_flights)

# Serve the chart's data via an asynchronous request (formerly known as 'AJAX')
@app.route("/total_flights.json")
def total_flights_json():
  total_flights = db.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return json_util.dumps(total_flights, ensure_ascii=False)

# Controller: Fetch a flight chart
@app.route("/total_flights_chart")
def total_flights_chart():
  total_flights = db.flights_by_month.find({}, 
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
  mongo_query = {}
  for item in search_config:
    field = item['field']
    value = request.args.get(field)
    print(field, value)
    arg_dict[field] = value
    if value:
      mongo_query[field] = value

  airplanes_cursor = db.airplanes.find(mongo_query).sort('Owner', 1).skip(start).limit(end - start)
  airplanes = list(airplanes_cursor)
  airplane_count = db.airplanes.count_documents(mongo_query)

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
  mfr_chart = db.airplane_manufacturer_totals.find_one()
  return json.dumps(mfr_chart)

# Controller: Fetch a flight and display it
@app.route("/airplane/<tail_number>")
@app.route("/airplane/flights/<tail_number>")
def flights_per_airplane(tail_number):
  flights = db.flights_per_airplane.find_one(
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
  airline_summary = db.airlines.find_one(
    {'CarrierCode': carrier_code}
  )
  airline_airplanes = db.airplanes_per_carrier.find_one(
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
  airlines = db.airplanes_per_carrier.find()
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

  mongo_query = {}
  if carrier:
    mongo_query['Carrier'] = carrier
  if flight_date:
    mongo_query['FlightDate'] = flight_date
  if origin:
    mongo_query['Origin'] = origin
  if dest:
    mongo_query['Dest'] = dest
  if tail_number:
    mongo_query['TailNum'] = tail_number
  if flight_number:
    mongo_query['FlightNum'] = flight_number

  flights_cursor = db.on_time_performance.find(mongo_query).sort([
    ('FlightDate', 1),
    ('DepTime', 1),
    ('Carrier', 1),
    ('FlightNum', 1)
  ]).skip(start).limit(end - start)
  flights = list(flights_cursor)
  flight_count = db.on_time_performance.count_documents(mongo_query)

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
  prediction_features['Distance'] = predict_utils.get_flight_distance(client, api_form_values['Origin'], api_form_values['Dest'])
  
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
    client, api_form_values['Origin'],
    api_form_values['Dest']
  )
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(
    api_form_values['FlightDate']
  )
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Add a timestamp
  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
  
  db.prediction_tasks.insert_one(
    prediction_features
  )
  return json_util.dumps(prediction_features)

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
  
  # Fetch today's prediction results from Mongo
  predictions = db.prediction_results.find(
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
    client, api_form_values['Origin'],
    api_form_values['Dest']
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
    p.flush(timeout=10)
    print(f"Prediction sent: {unique_id[:12]} to {PREDICTION_TOPIC}")
  except Exception as e:
    print(f"Kafka send error: {e}")

  response = {"status": "OK", "id": unique_id}
  return json_util.dumps(response)

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
  
  prediction = db.flight_delay_ml_response.find_one(
    {
      "UUID": unique_id
    }
  )
  
  response = {"status": "WAIT", "id": unique_id}
  if prediction:
    response["status"] = "OK"
    response["prediction"] = prediction
  
  return json_util.dumps(response)

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
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MODEL_FIXED_PATH = 's3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin'
MLFLOW_ARTIFACT_ROOT = 's3a://lakehouse/mlflow'

def _mlflow_request(method, path, data=None):
  import requests
  url = f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/{path}"
  r = requests.request(method, url, json=data, timeout=10)
  return r.json() if r.status_code == 200 else {}

@app.route("/models")
def models_page():
  runs = _mlflow_request("POST", "runs/search", {"experiment_ids": ["0"], "order_by": ["start_time desc"]})
  model_list = []
  for run in runs.get("runs", []):
    info = run["info"]
    data = run["data"]
    params = {p["key"]: p["value"] for p in data.get("params", [])}
    metrics = {m["key"]: m["value"] for m in data.get("metrics", [])}
    model_list.append({
      "run_id": info["run_id"],
      "run_name": info.get("run_name", "—"),
      "status": info["status"],
      "start_time": info["start_time"],
      "params": params,
      "metrics": metrics,
    })

  # Check which model is currently active by reading marker file from minio
  active_run_id = None
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
    active_run_id = "builtin"

  return render_template("models.html",
    models=model_list,
    active_run_id=active_run_id,
  )

@app.route("/models/activate/<run_id>")
def activate_model(run_id):
  run = _mlflow_request("GET", f"runs/get?run_id={run_id}")
  run_info = run.get("run", {})
  if not run_info:
    return "Run not found", 404

  try:
    import boto3
    from botocore.config import Config
    s3 = boto3.client("s3",
      endpoint_url=MINIO_ENDPOINT,
      aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
      aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
      config=Config(signature_version="s3v4"))

    src_prefix = f"mlflow/0/{run_id}/artifacts/model/"
    dst_prefix = "models/spark_random_forest_classifier.flight_delays.5.0.bin/"

    # Find the stage directory inside sparkml/stages/
    stages = s3.list_objects_v2(Bucket="lakehouse", Prefix=f"{src_prefix}sparkml/stages/", Delimiter="/")
    stage_prefix = None
    for p in stages.get("CommonPrefixes", []):
      stage_prefix = p["Prefix"]
      break

    if not stage_prefix:
      return "No model stages found in MLflow artifact", 400

    # Empty destination prefix
    existing = s3.list_objects_v2(Bucket="lakehouse", Prefix=dst_prefix)
    for obj in existing.get("Contents", []):
      s3.delete_object(Bucket="lakehouse", Key=obj["Key"])

    # Copy from stage directory to fixed model path
    objects = s3.list_objects_v2(Bucket="lakehouse", Prefix=stage_prefix)
    for obj in objects.get("Contents", []):
      src_key = obj["Key"]
      rel_path = src_key[len(stage_prefix):]
      dst_key = f"{dst_prefix}{rel_path}"
      s3.copy_object(
        Bucket="lakehouse",
        CopySource={"Bucket": "lakehouse", "Key": src_key},
        Key=dst_key)

    # Write active run marker
    s3.put_object(Bucket="lakehouse", Key="models/active_run_id.txt", Body=run_id.encode())

    return redirect(url_for("models_page"))
  except Exception as e:
    return f"Error activating model: {e}", 500

if __name__ == "__main__":
    socketio.run(
    app,
    debug=True,
    use_reloader=False,
    host='0.0.0.0',
    port=int(os.getenv('FLASK_PORT', '5001')),
    allow_unsafe_werkzeug=True
  )
