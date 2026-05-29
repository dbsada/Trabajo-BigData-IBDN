import os
import logging
import sys
import signal
from utils.shell import sh
from cloud.config import DeployConfig
from cloud import gcp_ops


def run_pipeline_gcloud(cfg):
    from rich.console import Console
    from rich.panel import Panel
    from rich import box

    console = Console()

    gcp = gcp_ops.GCPConfig()
    vm_running = False
    vm_stopped = False

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
            gcp_ops.create_vm(gcp)
            gcp_ops.wait_for_vm(gcp, timeout=180)
        else:
            gcp_ops.start_vm(gcp)
            gcp_ops.wait_for_vm(gcp, timeout=120)
        vm_running = True

        gcp_ops.deploy_code(gcp)
        gcp_ops.deploy_env(gcp)

        sbt_dir = os.path.join(cfg.project_home, "flight_prediction")
        _sbt_package(sbt_dir)

        jar_path = os.path.join(cfg.project_home, "flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar")
        gcp_ops.deploy_jar(gcp, jar_path)

        gcp_ops.deploy_down(gcp, cfg.db_mode)
        gcp_ops.deploy_pull(gcp, cfg.db_mode)
        gcp_ops.deploy_build(gcp, cfg.db_mode)
        gcp_ops.deploy_up(gcp, cfg.db_mode)

        gcp_ops.run_pipeline(gcp, cfg.db_mode, cfg)
        gcp_ops.start_prediction(gcp, cfg)

        vm_ip = gcp_ops.get_external_ip(gcp)
        gcp_ops.suggest_tunnel()
        gcp_ops.ensure_iap(gcp)

        console.print()
        console.print(Panel(
            f"[bold]Flask UI:[/bold]  [link=http://localhost:5001]http://localhost:5001[/link]\n"
            f"[bold]MinIO:[/bold]     [link=http://localhost:9001]http://localhost:9001[/link]\n"
            f"[bold]Spark UI:[/bold]  [link=http://{vm_ip}:8081]http://{vm_ip}:8081[/link]\n"
            f"[bold]MLflow:[/bold]    [link=http://localhost:5002]http://localhost:5002[/link]\n\n"
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
        if vm_running and not vm_stopped:
            vm_stopped = True
            gcp_ops.stop_vm(gcp)
            console.print("[dim]VM stopped. Goodbye![/dim]")


def run_pipeline_gke(cfg):
    from rich.console import Console
    console = Console()

    gcp = gcp_ops.GCPConfig()
    gcp_ops.create_gke_cluster(gcp)
    gcp_ops.deploy_k8s(gcp)
    gcp_ops.suggest_tunnel()


@sh
def _sbt_package(cwd):
    return f"cd {cwd} && sbt package"
