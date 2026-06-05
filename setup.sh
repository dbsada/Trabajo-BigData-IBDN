python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .

echo '#!/usr/bin/env bash
set -e
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHONPATH="$PROJECT_DIR" exec "$PROJECT_DIR/.venv/bin/python" -c "
import sys; sys.path.insert(0, \"$PROJECT_DIR\"); sys.argv[0]=\"predict\"
from cli import main; sys.exit(main())
" "$@"' > .venv/bin/predict && chmod +x .venv/bin/predict