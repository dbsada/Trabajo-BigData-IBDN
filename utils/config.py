import os

def load_dotenv(path=None):
    if path is None:
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    if not os.path.isfile(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())
