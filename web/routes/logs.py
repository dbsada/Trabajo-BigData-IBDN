"""Logs de los servicios del cluster."""

import json
import os
import subprocess

from flask import Blueprint, jsonify, request

bp = Blueprint("logs", __name__)


def _get_pod_logs(pod_label, tail=500, container=None):
    """Obtiene logs de un pod por label."""
    try:
        token = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read().strip()
        ca = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        host = os.getenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
        base = f"https://{host}:{port}"

        pods_r = subprocess.run(
            ["curl", "-s", "--cacert", ca, "-H", f"Authorization: Bearer {token}",
             f"{base}/api/v1/namespaces/ibdn/pods?labelSelector=app={pod_label}&fieldSelector=status.phase=Running"],
            capture_output=True, text=True, timeout=10,
        )
        items = json.loads(pods_r.stdout).get("items", [])
        if not items:
            return None, "Pod not found"

        items.sort(key=lambda p: p["metadata"]["creationTimestamp"], reverse=True)
        pod_name = items[0]["metadata"]["name"]
        url = f"{base}/api/v1/namespaces/ibdn/pods/{pod_name}/log?tailLines={tail}"
        if container:
            url += f"&container={container}"

        log_r = subprocess.run(
            ["curl", "-s", "--cacert", ca, "-H", f"Authorization: Bearer {token}", url],
            capture_output=True, text=True, timeout=10,
        )
        return log_r.stdout, None
    except Exception as e:
        return None, str(e)


@bp.route("/api/logs/<service>")
def service_logs(service):
    """Devuelve logs de un servicio específico."""
    tail = request.args.get("tail", 500, type=int)
    pod_map = {
        "kafka": "kafka",
        "spark": "spark-manager",
        "spark-worker": "spark-worker",
        "cassandra": "cassandra",
        "flask": "flask",
        "mlflow": "mlflow",
        "minio": "minio",
        "airflow-webserver": "airflow-webserver",
        "airflow-scheduler": "airflow-scheduler",
    }
    pod_label = pod_map.get(service)
    if not pod_label:
        return jsonify({"error": f"Unknown service: {service}"}), 404

    container = "webserver" if service == "airflow-webserver" else None
    logs, error = _get_pod_logs(pod_label, tail, container)
    if error:
        return jsonify({"error": error}), 500
    return jsonify({"logs": logs})


@bp.route("/api/logs/all")
def all_logs():
    """Devuelve logs de todos los servicios entrelazados."""
    services = ["kafka", "spark", "spark-worker", "cassandra", "flask", "mlflow", "minio"]
    result = {}
    for svc in services:
        logs, _ = _get_pod_logs(svc, tail=200)
        if logs:
            result[svc] = logs
    return jsonify(result)
