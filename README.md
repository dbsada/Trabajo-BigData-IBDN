# Flight Delay Prediction — IBDN Cluster

## Quick Start

```bash
# 1. Create virtual environment (first time only)
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 rich requests typer questionary

# 2. Setup f-pred command (instant, no pip install needed)
source setup.sh

# 3. Run
f-pred docker --db cassandra
```

## Commands

| Command | Description |
|---|---|
| `f-pred docker --db cassandra` | Deploy locally with Cassandra |
| `f-pred docker --db mongo` | Deploy locally with MongoDB |
| `f-pred gcloud-docker --db cassandra` | Deploy to GCloud VM |

## Architecture

- **spark-manager**: Spark master (scheduler, UI on port 8081)
- **spark-worker**: Spark worker (executes jobs)
- **flask**: Web UI + API (port 5001)
- **kafka**: Message broker (port 9092)
- **minio**: Object storage (port 9000/9001)
- **mlflow**: ML experiment tracking (port 5002)
- **cassandra/mongodb**: Database (port 9042/27017)
