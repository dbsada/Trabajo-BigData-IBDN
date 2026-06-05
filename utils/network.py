import socket
import time
import requests

def check_port(port, host='localhost', timeout=0.5):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def wait_for_port(port, host='localhost', timeout=20):
    for _ in range(timeout):
        if check_port(port, host, timeout=1):
            return True
        time.sleep(1)
    raise TimeoutError(f"Port {port} not available after {timeout} seconds.")

def wait_for_http(url, timeout=20):
    for _ in range(timeout):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code < 500:
                return True
        except (requests.ConnectionError, requests.Timeout):
            time.sleep(1)
    return False
