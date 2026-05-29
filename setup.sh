#!/bin/bash
# Quick setup: registers f-pred command and cloud/utils modules instantly
# Usage: source setup.sh
# This is instant and won't corrupt on Ctrl+C (unlike pip install -e .)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ ! -d "$SCRIPT_DIR/.venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$SCRIPT_DIR/.venv"
fi

VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"
SITE_PACKAGES=$("$VENV_PYTHON" -c "import site; print(site.getsitepackages()[0])")

# Install dependencies if not present
"$VENV_PYTHON" -c "import rich" 2>/dev/null || "$VENV_PYTHON" -m pip install -q boto3 rich requests typer questionary

# Create .pth file so cloud and utils are importable
echo "$SCRIPT_DIR" > "$SITE_PACKAGES/flight-prediction.pth"

# Create f-pred wrapper script
cat > "$SCRIPT_DIR/.venv/bin/f-pred" << PYEOF
#!/usr/bin/env python
import sys
sys.path.insert(0, "$SCRIPT_DIR")
from cloud.cli import main
main()
PYEOF
chmod +x "$SCRIPT_DIR/.venv/bin/f-pred"

echo "Setup complete. Run: source .venv/bin/activate && f-pred docker --db cassandra"
