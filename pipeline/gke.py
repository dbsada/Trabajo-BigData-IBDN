import os
import logging
import subprocess
import shlex
from utils.shell import sh
from config import DeployConfig
from pipeline import gcp_ops


def run_pipeline_gke(cfg):
    from rich.console import Console
    from rich.panel import Panel
    from rich import box
    console = Console()

    gcp = gcp_ops.GCPConfig()
    cluster_created = False

    def _cleanup_and_exit(delete_cluster=False):
        cluster_name = os.getenv('GKE_CLUSTER_NAME', 'ibdn-cluster')
        if delete_cluster and cluster_created:
            console.print()
            console.print("[yellow]Ctrl+C — deleting cluster...[/yellow]")
            subprocess.run(
                f"gcloud container clusters delete {cluster_name} "
                f"--zone {gcp.zone} --project {gcp.project} --quiet",
                shell=True
            )
            console.print("[green]Cluster deleted.[/green]")
        else:
            console.print()
            console.print("[red]GKE deployment failed. Cluster is still running.[/red]")
            console.print(
                f"[dim]Delete it manually:[/dim] [bold]gcloud container clusters delete "
                f"{cluster_name} "
                f"--zone {gcp.zone} --project {gcp.project} --quiet[/bold]"
            )

    try:
        machine_type = os.getenv('GKE_MACHINE_TYPE', 'e2-standard-4')
        num_nodes = os.getenv('GKE_NUM_NODES', '3')
        console.print(f"[bold]Step 1/5:[/bold] Creating GKE cluster ({machine_type} x {num_nodes} nodes)...")
        try:
            gcp_ops.create_gke_cluster(gcp)
        except RuntimeError as e:
            console.print(f"[red]{e}[/red]")
            return
        cluster_created = True

        console.print("[bold]Step 2/5:[/bold] Waiting for cluster to be ready...")
        gcp_ops.wait_for_gke_cluster(gcp)
        gcp_ops.scale_gke_cluster(gcp, int(os.getenv('GKE_NUM_NODES', '3')))

        console.print("[bold]Step 3/5:[/bold] Getting kubectl credentials...")
        gcp_ops.get_gke_credentials(gcp)

        console.print("[bold]Step 4/5:[/bold] Building and pushing Docker images...")
        gcp_ops.build_and_push_images(cfg)

        console.print("[bold]Step 5/5:[/bold] Deploying Kubernetes manifests...")
        gcp_ops.deploy_k8s(gcp, cfg)

        console.print("[bold green]Deployment submitted![/bold green]")
        console.print("[dim]Waiting for nodes to be ready...[/dim]")
        expected_nodes = int(os.getenv('GKE_NUM_NODES', '3'))
        for attempt in range(15):
            r = subprocess.run(
                "kubectl get nodes -o jsonpath='{.items[?(@.status.conditions[?(@.type==\"Ready\")].status==\"True\")].metadata.name}' 2>/dev/null | wc -w",
                shell=True, capture_output=True, text=True
            )
            if r.stdout.strip() and int(r.stdout.strip()) >= expected_nodes:
                console.print(f"[dim]  {r.stdout.strip()} nodes ready[/dim]")
                break
            console.print(f"[yellow]  Waiting for nodes (attempt {attempt+1}/15)...[/yellow]")
            subprocess.run("sleep 15", shell=True)
        console.print("[dim]Waiting for key pods to be ready...[/dim]")
        for app in ["spark-manager", "flask", "minio", "kafka", "cassandra"]:
            for attempt in range(5):
                r = subprocess.run(
                    f"kubectl wait --for=condition=ready pod -l app={app} -n ibdn --timeout=60s 2>/dev/null",
                    shell=True
                )
                if r.returncode == 0:
                    break
                console.print(f"[yellow]  Waiting for {app} (attempt {attempt+1}/5)...[/yellow]")
                subprocess.run("sleep 10", shell=True)
        console.print("[dim]Giving Spark master a moment to bind port 7077...[/dim]")
        subprocess.run("sleep 15", shell=True)

        # Helper for kubectl exec with retry
        def _kexec(cmd, timeout=120, retries=5):
            for attempt in range(retries):
                r = subprocess.run(
                    f"kubectl exec -n ibdn deploy/flask -- {cmd} 2>/dev/null",
                    shell=True, capture_output=True, timeout=timeout
                )
                if r.returncode == 0:
                    return r
                if attempt < retries - 1:
                    subprocess.run("sleep 5", shell=True)
            return r

        # Data pipeline
        console.print("[dim]Running data pipeline...[/dim]")
        _kexec("sh -c 'MINIO_LOCAL_ENDPOINT=http://minio:9000 python3 /app/scripts/create_bucket.py'", timeout=300)
        console.print("  Creating MinIO buckets...")
        _kexec("sh -c 'MINIO_LOCAL_ENDPOINT=http://minio:9000 python3 /app/scripts/download_data.py'", timeout=300)
        console.print("  Downloading flight data...")
        # Upload data
        subprocess.run(
            "kubectl exec -n ibdn deploy/minio -- mc alias set local http://localhost:9000 admin password 2>/dev/null; true",
            shell=True, timeout=15
        )
        for fname in ["simple_flight_delay_features.jsonl.bz2", "origin_dest_distances.jsonl"]:
            for attempt in range(5):
                r = subprocess.run(
                    f"kubectl exec -n ibdn deploy/minio -- mkdir -p /tmp/data 2>/dev/null; "
                    f"kubectl exec -n ibdn deploy/flask -- sh -c 'cat /app/data/{fname}' | "
                    f"kubectl exec -n ibdn -i deploy/minio -- sh -c 'cat > /tmp/data/{fname}' 2>/dev/null; "
                    f"kubectl exec -n ibdn deploy/minio -- mc cp /tmp/data/{fname} local/lakehouse/raw/ 2>/dev/null; true",
                    shell=True, timeout=60
                )
                if r.returncode == 0:
                    break
                subprocess.run("sleep 5", shell=True)
        # Kafka topics
        for topic in ["flight-delay-ml-request", "flight-delay-ml-response", "flight-delay-ml-status"]:
            subprocess.run(
                f"kubectl exec -n ibdn deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --create "
                f"--bootstrap-server localhost:9092 --topic {topic} --partitions 1 --replication-factor 1 "
                f"--if-not-exists 2>/dev/null; true", shell=True, timeout=30
            )
        console.print("[dim]Data pipeline complete.[/dim]")

        # Import distances
        console.print("  Importing distance data to Cassandra...")
        IMPORT_SCRIPT = (
            "import json;from cassandra.cluster import Cluster;"
            "c=Cluster(['cassandra'],port=9042);s=c.connect();"
            's.execute("CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = ' + "{'class': 'SimpleStrategy', 'replication_factor': 1}" + '");'
            "s.set_keyspace('agile_data_science');"
            "s.execute('CREATE TABLE IF NOT EXISTS origin_dest_distances (origin text,dest text,distance double,PRIMARY KEY(origin,dest))');"
            "s.execute('TRUNCATE origin_dest_distances');"
            "s.execute('CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response ("
            "uuid text PRIMARY KEY, prediction int, origin text, dest text, dep_delay double, "
            "carrier text, flight_date text, flight_num text, distance double, route text, "
            "day_of_year int, day_of_month int, day_of_week int, timestamp text)');"
            "n=0;"
            "with open('/app/data/origin_dest_distances.jsonl') as f:"
            " for line in f:"
            "  r=json.loads(line);"
            "  s.execute('INSERT INTO origin_dest_distances(origin,dest,distance)VALUES(%s,%s,%s)',(r['Origin'],r['Dest'],r['Distance']));"
            "  n+=1;"
            "print(f'{n} records')"
        )
        for attempt in range(5):
            r = subprocess.run(
                ["kubectl", "exec", "-n", "ibdn", "deploy/flask", "--", "python3", "-c", IMPORT_SCRIPT],
                capture_output=True, text=True, timeout=120
            )
            if r.returncode == 0:
                print(f"    {r.stdout.strip()}")
                break
            if attempt < 4:
                console.print(f"[yellow]  Cassandra not ready yet (attempt {attempt+1}/5)...[/yellow]")
                subprocess.run("sleep 10", shell=True)
        console.print("[dim]Distances imported.[/dim]")

        # Start prediction via REST API (from within the cluster)
        console.print("[dim]Starting prediction job via REST API...[/dim]")
        subprocess.run(
            'kubectl exec -n ibdn deploy/flask -- curl -s -X POST http://spark-manager:6066/v1/submissions/create '
            "-H 'Content-Type: application/json' "
            '-d \'{"action":"CreateSubmissionRequest","appArgs":[],'
            '"appResource":"file:/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar",'
            '"clientSparkVersion":"4.1.1","mainClass":"es.upm.dit.ging.predictor.MakePrediction",'
            '"sparkProperties":{'
            '"spark.master":"spark://spark-manager:7077",'
            '"spark.submit.deployMode":"cluster",'
            '"spark.cores.max":"2",'
            '"spark.driver.memory":"2g","spark.executor.memory":"2g",'
            '"spark.hadoop.fs.s3a.access.key":"admin",'
            '"spark.hadoop.fs.s3a.secret.key":"password",'
            '"spark.hadoop.fs.s3a.endpoint":"http://minio:9000",'
            '"spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem",'
            '"spark.hadoop.fs.s3a.path.style.access":"true",'
            '"spark.hadoop.fs.s3a.connection.ssl.enabled":"false",'
            '"spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",'
            '"spark.sql.catalog.lakehouse":"org.apache.iceberg.spark.SparkCatalog",'
            '"spark.sql.catalog.lakehouse.type":"hadoop",'
            '"spark.sql.catalog.lakehouse.io-impl":"org.apache.iceberg.hadoop.HadoopFileIO",'
            '"spark.sql.catalog.lakehouse.warehouse":"s3a://lakehouse",'
            '"spark.sql.catalog.lakehouse.s3.endpoint":"http://minio:9000",'
            '"spark.sql.catalog.lakehouse.s3.access-key":"admin",'
            '"spark.sql.catalog.lakehouse.s3.secret-key":"password",'
            '"spark.sql.catalog.lakehouse.s3.path-style.access":"true",'
            '"spark.sql.defaultCatalog":"lakehouse",'
            '"spark.executorEnv.MLFLOW_TRACKING_URI":"http://mlflow:5000",'
            '"spark.driverEnv.MODEL_VERSION":"1.0",'
            '"spark.driverEnv.BUCKETIZER_VERSION":"1.0"}}\'',
            shell=True
        )
        gcp_ops.port_forward_gke()

    except KeyboardInterrupt:
        console.print()
        _cleanup_and_exit(delete_cluster=True)
        return
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logging.error("GKE deploy failed", exc_info=True)
        _cleanup_and_exit()


@sh
def _sbt_package(cwd):
    return f"cd {shlex.quote(cwd)} && sbt package"
