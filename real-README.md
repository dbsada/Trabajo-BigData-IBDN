# Flight Delay Prediction — Práctica IBDN

## Quick Start

```bash
# 1. Crear entorno virtual
python3 -m venv .venv
source .venv/bin/activate

# 2. Instalar el comando f-pred (modo editable, no contamina el sistema)
pip install -e .

# 3. Desplegar
f-pred docker --db cassandra
```

## ¿Qué hace `pip install -e .`?

Crea un script `f-pred` dentro de `.venv/bin/`. **Solo existe cuando el venv está activado.** No copia archivos, no toca `/usr/local/bin`, no instala nada global.

Es un enlace simbólico a tu código. Si borras el proyecto, el comando deja de funcionar. Si desinstalas, desaparece.

## Comandos disponibles

| Comando | Descripción |
|---|---|
| `f-pred docker --db cassandra` | Deploy local con Cassandra |
| `f-pred docker --db mongo` | Deploy local con MongoDB |
| `f-pred gcloud-docker --db cassandra` | Deploy en VM de GCloud con Docker |
| `f-pred gcloud-kubernetes` | Deploy en GKE |

## Desinstalar

```bash
pip uninstall flight-prediction-orchestrator -y
```

Borra el script de `.venv/bin/`. Tus archivos intactos.

## Estructura del proyecto

```
PRACTICA_CREATIVA/
├── cloud/
│   ├── cli.py              ← CLI con typer (f-pred)
│   ├── config.py           ← DeployConfig dataclass
│   ├── orchestrator.py     ← Orquestador principal
│   ├── docker_ops.py       ← Operaciones Docker Compose
│   ├── spark_ops.py        ← Operaciones Spark
│   ├── kafka_ops.py        ← Operaciones Kafka
│   ├── gcp_ops.py          ← Operaciones GCloud/SSH
│   ├── pipeline.py         ← Pipeline local
│   └── pipeline_gcloud.py  ← Pipeline GCloud SSH
├── utils/
│   ├── config.py           ← Carga de .env
│   ├── shell.py            ← Decorador @sh
│   ├── logs.py             ← Agregación de logs
│   ├── minio.py            ← Cliente MinIO/S3
│   ├── spark.py            ← Comandos spark-submit
│   └── network.py          ← Helpers de red
├── scripts/
│   ├── web/                ← Servidor Flask
│   ├── legacy/train.py     ← PySpark original (profesores)
│   ├── create_bucket.py
│   ├── download_data.py
│   └── import_distances.py
├── flight_prediction/      ← Proyecto Scala/SBT
├── docker/                 ← Dockerfiles
├── docker-compose.yaml
└── pyproject.toml
```

## Requisitos previos

- Docker Desktop instalado y corriendo
- Python 3.7+
- `gcloud` CLI (solo para deploy en GCloud)
