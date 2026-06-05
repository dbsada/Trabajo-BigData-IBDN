import os
import logging
import sys
import signal
import subprocess
import time as _time
from utils.shell import sh
from config import DeployConfig
from pipeline import gcp_ops


def run_pipeline_gcloud(cfg):
    from rich.console import Console
    from rich.panel import Panel
    from rich import box

    console = Console()

    gcp = gcp_ops.GCPConfig()
    vm_running = False
    vm_stopped = False
    succeeded = False

    def _stop_vm_on_exit(signum=None, frame=None):
        nonlocal vm_stopped
        if vm_running and not vm_stopped:
            vm_stopped = True
            console.print()
            console.print("[red]Signal received. Stopping VM...[/red]")
            try:
                gcp_ops.stop_vm(gcp)
                console.print("[dim]VM stopped.[/dim]")
            except Exception as e:
                console.print(f"[yellow]Warning: could not stop VM: {e}[/yellow]")
        sys.exit(1)

    signal.signal(signal.SIGINT, _stop_vm_on_exit)
    signal.signal(signal.SIGTERM, _stop_vm_on_exit)

    try:
        exists = gcp_ops.vm_exists(gcp)
        if not exists:
            console.print("[bold]Step 1/5:[/bold] Creating VM...")
            gcp_ops.create_vm(gcp)
            gcp_ops.wait_for_vm(gcp, timeout=180)
        else:
            console.print("[bold]Step 1/5:[/bold] Starting VM...")
            gcp_ops.start_vm(gcp)
            gcp_ops.wait_for_vm(gcp, timeout=120)
        vm_running = True

        console.print("[bold]Step 2/5:[/bold] Deploying code to VM...")
        gcp_ops.deploy_code(gcp)
        gcp_ops.deploy_env(gcp)

        sbt_dir = os.path.join(cfg.project_home, "flight_prediction")
        _sbt_package(sbt_dir)

        jar_path = os.path.join(cfg.project_home, "flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar")
        gcp_ops.deploy_jar(gcp, jar_path)

        console.print("[bold]Step 3/5:[/bold] Building and starting services on VM...")
        console.print("[dim]  (Spark image build may take 5-10 min on first deploy)[/dim]")
        gcp_ops._send_progress(gcp, "core_services", "running", "Starting all services...")
        gcp_ops.deploy_down(gcp, cfg.db_mode)
        gcp_ops.deploy_compose(gcp, cfg.db_mode)
        # Wait for Flask to be ready before sending progress
        flk_port = os.getenv('FLASK_PORT', '5001')
        for _ in range(20):
            r = gcp_ops._ssh_run_cmd(gcp, f"docker exec flask curl -s --connect-timeout 2 --max-time 3 http://localhost:{flk_port}/api/pipeline/progress >/dev/null 2>&1 && echo OK || true")
            if r.returncode == 0 and 'OK' in r.stdout:
                break
            _time.sleep(3)
        gcp_ops._send_progress(gcp, "core_services", "done", "Core services ready")
        gcp_ops._send_progress(gcp, "infra_services", "done", "All services ready")
        console.print("[dim]  Services started.[/dim]")

        console.print("[bold]Step 4/5:[/bold] Running pipeline...")
        # Start a background SSH tunnel for Flask so user can see progress live
        tunnel_proc = subprocess.Popen(
            ["gcloud", "compute", "ssh", f"{gcp.user}@{gcp.instance}",
             "--zone", gcp.zone, "--project", gcp.project, "--quiet", "--",
             "-N", "-L", f"{flk_port}:localhost:{flk_port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        _time.sleep(5)
        console.print("[dim]  Open [link=http://localhost:5001]http://localhost:5001[/link] in your browser to see progress live.[/dim]")
        _time.sleep(3)
        gcp_ops.run_pipeline(gcp, cfg.db_mode, cfg)
        gcp_ops._send_progress(gcp, "prediction", "running", "Starting prediction engine...")
        gcp_ops.start_prediction(gcp, cfg)
        gcp_ops._send_progress(gcp, "prediction", "done", "Prediction engine started")
        gcp_ops._send_progress(gcp, "done", "done", "Ready")
        # Kill the background tunnel
        tunnel_proc.terminate()
        try:
            tunnel_proc.wait(timeout=5)
        except Exception:
            tunnel_proc.kill()

        succeeded = True

        console.print("[bold]Step 5/5:[/bold] Setting up tunnels...")
        vm_ip = gcp_ops.get_external_ip(gcp)
        try:
            gcp_ops.ensure_iap(gcp)
        except Exception:
            pass

        console.print()
        spark_url = f"http://{vm_ip}:8081" if vm_ip and vm_ip != "N/A" else "http://localhost:8081"
        console.print(Panel(
            f"[bold]Flask UI:[/bold]  [link=http://localhost:5001]http://localhost:5001[/link]\n"
            f"[bold]Airflow:[/bold]   [link=http://localhost:8085]http://localhost:8085[/link]\n"
            f"[bold]MinIO:[/bold]     [link=http://localhost:9001]http://localhost:9001[/link]\n"
            f"[bold]Spark UI:[/bold]  [link={spark_url}]{spark_url}[/link]\n"
            f"[bold]MLflow:[/bold]    [link=http://localhost:5003]http://localhost:5003[/link]\n\n"
            f"[dim]Open Flask in your browser to see progress and logs.[/dim]",
            title="[bold green]✓ GCloud Ready[/bold green]",
            border_style="green",
            box=box.ROUNDED,
            expand=False,
        ))
        console.print("[dim]Press Ctrl+C to stop tunnels and shut down VM[/dim]")

        gcp_ops.tunnel(gcp)

    except KeyboardInterrupt:
        console.print()
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logging.error("GCloud deploy failed", exc_info=True)
    finally:
        if vm_running and not vm_stopped and not succeeded:
            vm_stopped = True
            gcp_ops.stop_vm(gcp)
            console.print("[dim]VM stopped. Goodbye![/dim]")


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
        console.print("[bold]Step 1/5:[/bold] Creating GKE cluster (e2-standard-4 x 3 nodes)...")
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
        gcp_ops.deploy_k8s(gcp)

        console.print("[bold green]Deployment submitted![/bold green]")
        console.print("[dim]Restarting pods to pick up latest images...[/dim]")
        subprocess.run(
            "kubectl rollout restart deployment -n ibdn 2>/dev/null; true",
            shell=True
        )
        console.print("[dim]Waiting for old Spark pods to terminate...[/dim]")
        subprocess.run(
            "kubectl wait --for=delete pod -l app=spark-manager -n ibdn --timeout=300s 2>/dev/null; true",
            shell=True
        )
        subprocess.run(
            "kubectl wait --for=condition=ready pod -l app=spark-manager -n ibdn --timeout=300s",
            shell=True
        )
        console.print("[dim]Giving Spark master a moment to bind port 7077...[/dim]")
        subprocess.run("sleep 15", shell=True)

        # Data pipeline: create buckets, upload data, create topics
        console.print("[dim]Running data pipeline...[/dim]")
        kexec = "kubectl exec -n ibdn deploy/flask --"
        for cmd, label in [
            (f"{kexec} sh -c 'MINIO_LOCAL_ENDPOINT=http://minio:9000 python3 /app/scripts/create_bucket.py' 2>/dev/null; true", "Creating MinIO buckets"),
            (f"{kexec} sh -c 'MINIO_LOCAL_ENDPOINT=http://minio:9000 python3 /app/scripts/download_data.py' 2>/dev/null; true", "Downloading flight data"),
        ]:
            console.print(f"  {label}...")
            subprocess.run(cmd, shell=True, timeout=300)
        # Upload data to MinIO
        for fname in ["simple_flight_delay_features.jsonl.bz2", "origin_dest_distances.jsonl"]:
            subprocess.run(
                f"kubectl exec -n ibdn deploy/minio -- mkdir -p /tmp/data 2>/dev/null; "
                f"kubectl cp data/{fname} ibdn/$(kubectl get pod -n ibdn -l app=minio -o name | head -1):/tmp/data/ 2>/dev/null; "
                f"kubectl exec -n ibdn deploy/minio -- mc cp /tmp/data/{fname} local/lakehouse/raw/ 2>/dev/null; true",
                shell=True, timeout=60
            )
        subprocess.run(f"{kexec} python3 /app/scripts/import_distances.py 2>/dev/null; true", shell=True, timeout=60)
        # Create Kafka topics
        for topic in ["flight-delay-ml-request", "flight-delay-ml-response", "flight-delay-ml-status"]:
            subprocess.run(
                f"kubectl exec -n ibdn deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --create "
                f"--bootstrap-server localhost:9092 --topic {topic} --partitions 1 --replication-factor 1 "
                f"--if-not-exists 2>/dev/null; true", shell=True, timeout=30
            )
        console.print("[dim]Data pipeline complete.[/dim]")

        # Import distances to Cassandra (separate due to complex piping)
        console.print("  Importing distance data to Cassandra...")
        subprocess.run(
            f"kubectl exec -n ibdn deploy/cassandra -- cqlsh -e "
            f"\"CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}\" "
            f"2>/dev/null; true",
            shell=True, timeout=30
        )
        IMPORT_SCRIPT = (
            "import json,sys;"
            "from cassandra.cluster import Cluster;"
            "c=Cluster(['cassandra'],port=9042);s=c.connect();"
            "s.set_keyspace('agile_data_science');"
            "s.execute('CREATE TABLE IF NOT EXISTS origin_dest_distances (origin text,dest text,distance double,PRIMARY KEY(origin,dest))');"
            "s.execute('TRUNCATE origin_dest_distances');"
            "n=0;"
            "for l in sys.stdin:"
            " r=json.loads(l);"
            " s.execute('INSERT INTO origin_dest_distances(origin,dest,distance)VALUES(%s,%s,%s)',(r['Origin'],r['Dest'],r['Distance']));"
            " n+=1;"
            "print(f'{n} records')"
        )
        distances_file = os.path.join(cfg.project_home, "data/origin_dest_distances.jsonl")
        if os.path.exists(distances_file):
            with open(distances_file) as f:
                r = subprocess.run(
                    ["kubectl", "exec", "-n", "ibdn", "-i", "deploy/flask", "--", "python3", "-c", IMPORT_SCRIPT],
                    input=f.read(), capture_output=True, text=True, timeout=60
                )
                if r.returncode == 0:
                    print(f"    {r.stdout.strip()}")
        console.print("[dim]Distances imported.[/dim]")

        # Check if a trained model exists before starting prediction
        model_check = subprocess.run(
            "kubectl exec -n ibdn deploy/minio -- sh -c 'mc ls local/lakehouse/models/arrival_bucketizer_2.0.bin/_SUCCESS 2>/dev/null && echo YES || echo NO'",
            shell=True, capture_output=True, text=True, timeout=15
        )
        has_model = 'YES' in model_check.stdout

        console.print("[dim]Starting prediction job...[/dim]")
        subprocess.run(
            "kubectl exec -n ibdn deploy/spark-manager -- "
            "spark-submit --master spark://spark-manager:7077 "
            "--deploy-mode cluster --conf spark.cores.max=2 "
            "--conf spark.driver.memory=2g --conf spark.executor.memory=2g "
            "--conf spark.hadoop.fs.s3a.access.key=admin "
            "--conf spark.hadoop.fs.s3a.secret.key=password "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            "--class es.upm.dit.ging.predictor.MakePrediction "
            "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar",
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
    return f"cd {cwd} && sbt package"
