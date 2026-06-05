#!/bin/bash
# Keep GKE port-forwards alive
PORTS=(
  "5001:5001:flask"
  "5003:5000:mlflow"
  "8085:8080:airflow-webserver"
  "9001:9000:minio"
  "8080:8080:spark-manager"
)

for entry in "${PORTS[@]}"; do
  IFS=':' read -r local remote svc <<< "$entry"
  while true; do
    kubectl port-forward -n ibdn "deploy/$svc" "$local:$remote" &>/tmp/pf-$svc.log
    echo "[$(date +%H:%M:%S)] $svc port-forward died, restarting in 3s..."
    sleep 3
  done &
done

echo "Port forwards active. Press Ctrl+C to stop."
wait
