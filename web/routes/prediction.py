from flask import Blueprint, request, jsonify
import kafka_handler
import json
import os
import bz2

bp = Blueprint("prediction", __name__)

_airport_cache = None


@bp.route("/api/airports")
def api_airports():
    """Devuelve lista de aeropuertos para autocomplete."""
    global _airport_cache
    if _airport_cache is None:
        data_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "simple_flight_delay_features.jsonl.bz2")
        if os.path.exists(data_file):
            codes = set()
            with bz2.open(data_file, "rt") as f:
                for line in f:
                    r = json.loads(line)
                    codes.add(r.get("Origin"))
                    codes.add(r.get("Dest"))
            _airport_cache = sorted(codes)
        else:
            _airport_cache = ["ATL", "SFO", "JFK", "LAX", "ORD", "DFW", "DEN", "MIA", "SEA", "BOS"]
    q = request.args.get("q", "").upper()
    match = [a for a in _airport_cache if a.startswith(q)] if q else _airport_cache
    return jsonify(match[:50])


@bp.route("/api/predict", methods=["POST"])
def predict():
    """Recibe el formulario + model_ids[], envía a Kafka, devuelve UUID."""
    data = request.form.to_dict()
    model_ids = request.form.getlist("model_ids[]") or [data.pop("model_id", "")]
    uuid = kafka_handler.send_prediction(data, model_ids)
    return jsonify({"status": "OK", "id": uuid})