import hashlib
import re
import threading
from datetime import datetime, timedelta

_spark_line_cache = {}
_spark_last_app_id = None
_spark_cache_lock = threading.Lock()

SERVICE_COLORS = {
    'kafka': '#9c36b5',
    'spark-manager': '#fdc41b',
    'spark-worker': '#e05a5a',
    'cassandra': '#2f9e44',
    'mongodb': '#2f9e44',
    'flask': '#1971c2',
    'minio': '#e05a5a',
    'mlflow': '#60a5fa',
}

def _get_spark_app_id():
    try:
        import requests as req
        r = req.get("http://spark-manager:8080/json/", timeout=3)
        for app in r.json().get("activeapps", []):
            if "FlightDelayPrediction" in app.get("name", ""):
                return app.get("id", "")
    except Exception:
        pass
    return None

def _collect_docker_logs(client, services, db_mode):
    all_lines = []
    service_list = list(services)
    if db_mode == 'mongodb':
        service_list.append('mongodb')
    else:
        service_list.append('cassandra')

    for name in service_list:
        try:
            container = client.containers.get(name)
            logs = container.logs(timestamps=True)
            text = logs.decode('utf-8', errors='replace')
            for line in text.split('\n'):
                if not line.strip():
                    continue
                if name == 'flask' and '/api/logs/' in line:
                    continue
                ts_match = re.match(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z\s(.*)', line)
                if ts_match:
                    ts_str = ts_match.group(1)[:26]
                    content = ts_match.group(2)
                    try:
                        ts = datetime.strptime(ts_str + 'Z', '%Y-%m-%dT%H:%M:%S.%fZ')
                    except Exception:
                        ts = datetime.min
                    content_hash = hashlib.md5((name + ':' + content).encode()).hexdigest()
                    all_lines.append((ts, name, SERVICE_COLORS.get(name, '#888'), content, content_hash))
                else:
                    content_hash = hashlib.md5((name + ':' + line).encode()).hexdigest()
                    all_lines.append((datetime.min, name, SERVICE_COLORS.get(name, '#888'), line, content_hash))
        except Exception:
            pass

    return all_lines

def _collect_spark_stdout(client, all_lines):
    global _spark_last_app_id
    try:
        container = client.containers.get('spark-worker')
        result = container.exec_run("sh -c 'ls -t /opt/spark/work/driver-*/stdout 2>/dev/null | head -1'")
        if result.exit_code != 0 or not result.output.strip():
            return
        latest_stdout = result.output.decode().strip()

        current_app_id = _get_spark_app_id()
        if current_app_id != _spark_last_app_id:
            with _spark_cache_lock:
                _spark_line_cache.clear()
            _spark_last_app_id = current_app_id

        tail_result = container.exec_run(f'tail -50 {latest_stdout}')
        if tail_result.exit_code == 0:
            driver_text = tail_result.output.decode('utf-8', errors='replace')
            spark_lines = [l for l in driver_text.split('\n') if l.strip() and l.startswith('[SPARK]')]
            with _spark_cache_lock:
                now = datetime.now()
                for line in spark_lines:
                    content_hash = 'sp_' + hashlib.md5(line.encode()).hexdigest()
                    if content_hash not in _spark_line_cache:
                        _spark_line_cache[content_hash] = (now, line)
                    ts, _ = _spark_line_cache[content_hash]
                    all_lines.append((ts, 'spark-worker', SERVICE_COLORS['spark-worker'], line, content_hash))
                if len(_spark_line_cache) > 500:
                    sorted_hashes = sorted(_spark_line_cache, key=lambda k: _spark_line_cache[k][0])
                    for h in sorted_hashes[:200]:
                        del _spark_line_cache[h]

        stderr_result = container.exec_run("sh -c 'ls -t /opt/spark/work/driver-*/stderr 2>/dev/null | head -1'")
        if stderr_result.exit_code == 0 and stderr_result.output.strip():
            latest_stderr = stderr_result.output.decode().strip()
            tail_err = container.exec_run(f'tail -30 {latest_stderr}')
            if tail_err.exit_code == 0:
                err_text = tail_err.output.decode('utf-8', errors='replace')
                err_lines = [l for l in err_text.split('\n') if l.strip() and ('ERROR' in l or 'Exception' in l or 'WARN' in l)]
                with _spark_cache_lock:
                    now = datetime.now()
                    for line in err_lines[-15:]:
                        content_hash = 'err_' + hashlib.md5(line.encode()).hexdigest()
                        if content_hash not in _spark_line_cache:
                            _spark_line_cache[content_hash] = (now, line)
                        ts, _ = _spark_line_cache[content_hash]
                        all_lines.append((ts, 'spark-worker', '#e05a5a', '[ERR] ' + line, content_hash))
    except Exception:
        pass

def _render_lines(all_lines):
    interleaved = []
    for ts, svc, color, content, content_hash in all_lines:
        ts_str = ts.strftime('%H:%M:%S') if ts != datetime.min else '--:--:--'
        tag = '<span class="log-time">' + ts_str + '</span>'
        tag += '<span class="log-service-tag" style="background:' + color + '20;color:' + color + '">' + svc + '</span>'
        tag += content
        tag = '<span class="log-line" data-hash="' + content_hash + '">' + tag + '</span>'
        interleaved.append(tag)
    return '\n'.join(interleaved)

class Logs:
    @staticmethod
    def get_all_logs(db_mode):
        import docker
        base_services = ['kafka', 'spark-manager', 'spark-worker', 'flask', 'minio', 'mlflow']
        try:
            client = docker.from_env()
            all_lines = _collect_docker_logs(client, base_services, db_mode)
            _collect_spark_stdout(client, all_lines)
            all_lines.sort(key=lambda x: x[0])
            all_lines = all_lines[-1000:]
            html = _render_lines(all_lines)
            return {"logs": html, "interleaved": True, "count": len(all_lines)}
        except Exception as e:
            return {"error": str(e), "logs": "", "interleaved": False}

    @staticmethod
    def get_service_logs(service, tail=500):
        import docker
        try:
            client = docker.from_env()
            container = client.containers.get(service)
            logs = container.logs(tail=tail, timestamps=False)
            text = logs.decode('utf-8', errors='replace')
            return {"logs": text, "service": service, "status": container.status}
        except docker.errors.NotFound:
            return {"error": f"Container {service} not found", "logs": ""}
        except Exception as e:
            return {"error": str(e), "logs": ""}
