"""Estado del cluster GKE (nodos, pods, escalado)."""

import json
import os
import subprocess

from flask import Blueprint, jsonify, request

bp = Blueprint("gke", __name__)


@bp.route("/api/gke/status")
def gke_status():
    """Obtiene nodos y pods del cluster GKE usando la API de K8s desde dentro del pod."""
    if os.getenv("DEPLOY_MODE", "") != "gke":
        return jsonify({"nodes": [], "pods": [], "error": "Only available in GKE mode"})

    result = {"nodes": [], "pods": [], "error": None}
    try:
        token = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read().strip()
        ca = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        host = os.getenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
        base = f"https://{host}:{port}"

        def k8s_get(path):
            return subprocess.run(
                ["curl", "-s", "--cacert", ca, "-H", f"Authorization: Bearer {token}", f"{base}{path}"],
                capture_output=True, text=True, timeout=10,
            )

        r = k8s_get("/api/v1/nodes")
        if r.returncode == 0:
            nodes = json.loads(r.stdout).get("items", [])
            palette = ["#6366f1", "#ec4899", "#06b6d4", "#f59e0b", "#8b5cf6"]
            for i, n in enumerate(nodes):
                short = n["metadata"]["name"].split("-")[-1][:5]
                s = n.get("status", {})
                alloc = s.get("allocatable", s.get("capacity", {}))
                mem = alloc.get("memory", "?")
                if mem and mem.endswith("Ki"):
                    mem = f"{int(mem[:-2]) // (1024*1024)}Gi"
                result["nodes"].append({
                    "name": n["metadata"]["name"],
                    "short_id": short,
                    "color": palette[i % len(palette)],
                    "ready": any(c.get("type") == "Ready" and c.get("status") == "True" for c in s.get("conditions", [])),
                    "cpu": alloc.get("cpu", "?"),
                    "memory": mem,
                    "instance_type": n["metadata"]["labels"].get("node.kubernetes.io/instance-type", "?"),
                })

        r2 = k8s_get("/api/v1/namespaces/ibdn/pods")
        if r2.returncode == 0:
            pods = json.loads(r2.stdout).get("items", [])
            node_colors = {n["name"]: n["color"] for n in result["nodes"]}
            for p in pods:
                cs = p.get("status", {}).get("containerStatuses", [])
                ready = sum(1 for c in cs if c.get("ready"))
                total = len(cs)
                node_name = p.get("spec", {}).get("nodeName", "")
                result["pods"].append({
                    "name": p["metadata"]["name"],
                    "ready": f"{ready}/{total}",
                    "status": p.get("status", {}).get("phase", "?"),
                    "restarts": sum(c.get("restartCount", 0) for c in cs),
                    "node": node_name.split("-")[-1][:5] if node_name else "?",
                    "node_color": node_colors.get(node_name, "#555"),
                })
    except Exception as e:
        result["error"] = str(e)

    return jsonify(result)
