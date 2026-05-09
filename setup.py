import os
import sys
import subprocess
import logging
import socket
import time
from enum import Enum
import rich
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich import box
import questionary
from prompt_toolkit.shortcuts import CompleteStyle
from typing import Literal
import requests

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/orchestrator.log")
    ]
)

_status_line = ""
console = Console()

def set_status(text):
    global _status_line
    if _status_line:
        sys.stdout.write(f"\033[1A\033[K{text}\n")
    else:
        sys.stdout.write(f"{text}\n")
    sys.stdout.flush()
    _status_line = text

class ClusterManager:
  def __init__(self):
    self.home = os.path.expanduser('~')
    self.project_home = os.path.join(self.home, os.getenv('PROJECT_HOME', 'ibdn').rstrip('/'))
    self.venv_python = os.path.join(self.project_home, '.venv/bin/python3')

    self.docker = self.DockerMode(self)
    # self.kubernetes = self.KubernetesMode(self)

  def _run_command(self, command, cwd=None, wait=True, start_new_session=False):
    if wait:
      process = subprocess.run(command, shell=True, cwd=cwd, capture_output=True, text=True)
      
      if process.returncode != 0:
        console = Console()
        
        # Recopilamos el error de donde venga
        raw_error = process.stderr.strip() or process.stdout.strip() or "No hay mensaje de error (stdout/stderr vacíos)"
        
        error_msg = f"[bold red]Comando:[/bold red] [white]{command}[/white]\n"
        error_msg += f"[bold yellow]Directorio:[/bold yellow] [white]{cwd or os.getcwd()}[/white]\n"
        error_msg += f"[bold yellow]Código de salida:[/bold yellow] [white]{process.returncode}[/white]\n"
        error_msg += f"[hr]\n[bold cyan]Mensaje de error:[/bold cyan]\n[green]{raw_error}[/green]"
        
        console.print(Panel(error_msg, title="[bold red]❌ Fallo detectado[/bold red]", border_style="red", expand=False))
        return None
      
      logging.info(f'Éxito: {command}')
      return process.stdout
    else:
      log_name = f'logs/{command.split("/")[-1].split(" ")[0]}.log'
      log_file = open(log_name, 'w')
      logging.info(f'Lanzando servicio en segundo plano: {command} (Log: {log_name})')
      return subprocess.Popen(command, shell=True, cwd=cwd, stdout=log_file, stderr=log_file, start_new_session=start_new_session)

  def run_local_script(self, script_name):
    script_path = os.path.join(self.project_home, 'scripts', script_name)
    return self._run_command(f"{self.venv_python} {script_path}")

  def _wait_for_port(self, port, timeout=20):
    '''
    Verifica si el puerto está listo antes de seguir.
    '''
    for _ in range(timeout):
      try:
        with socket.create_connection(('localhost', port), timeout=1):
          return True
      except (ConnectionRefusedError, socket.timeout):
        time.sleep(1)
    raise TimeoutError(f'Puerto {port} no disponible después de {timeout} segundos.')

  def _wait_for_http(self, url, timeout=20):
    logging.info(f"⏳ Esperando a que la API responda en {url}...")
    for _ in range(timeout):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
        except requests.ConnectionError:
            time.sleep(1)
    return False

  def _check_port(self, port, host='localhost', timeout=0.5):
    try:
      with socket.create_connection((host, port), timeout=timeout):
        return True
    except:
      return False

  def _get_vm_ip(self):
    try:
      return subprocess.run(
        ['hostname', '-I'], capture_output=True, text=True, check=True
      ).stdout.strip().split()[0]
    except:
      return 'localhost'

  def _build_svc_table(self, services):
    vm_ip = self._get_vm_ip()
    table = Table(box=box.ROUNDED, title="Starting Services")
    table.add_column("Service", style="cyan", min_width=12)
    table.add_column("Status", justify="center", min_width=6, max_width=8)
    table.add_column("Port", style="dim", justify="center", min_width=6, max_width=8)
    table.add_column("URL", style="dim", justify="center", min_width=20)
    for s in services:
      status = s.get("status", "·")
      url = ""
      if s["name"] in ("Flask", "MinIO", "Spark"):
        url = f"http://{vm_ip}:{s['port']}"
      table.add_row(s["name"], status, str(s["port"]) if s.get("port") else "-", url)
    return table
    
  class DockerMode:
    def __init__(self, manager):
      self.m = manager
      self.db = self.Database(manager)
      self.spark = self.Spark(manager)
      self.kafka = self.Kafka(manager)
    
    def start_services(self, db: Literal['mongo', 'cassandra']):
      global _status_line
      _status_line = ""
      logging.info('🚀 Iniciando servicios con Docker Compose...')
      os.environ['DB_MODE'] = db
      self.m._run_command(f'docker compose --profile db_{db} up -d', cwd=self.m.project_home)

      db_label = "MongoDB" if db == 'mongo' else "Cassandra"
      db_port = int(os.getenv('MONGODB_PORT', '27017')) if db == 'mongo' else int(os.getenv('CASSANDRA_PORT', '9042'))

      services = [
        {"name": "Kafka",   "port": int(os.getenv('KAFKA_PORT', '9092')),         "ready": False},
        {"name": db_label,  "port": db_port,                                        "ready": False},
        {"name": "Spark",   "port": int(os.getenv('SPARK_MASTER_UI_PORT', '8080')),"ready": False},
        {"name": "Flask",   "port": int(os.getenv('FLASK_PORT', '5001')),           "ready": False},
        {"name": "MinIO",   "port": int(os.getenv('MINIO_API_PORT', '9000')),       "ready": False},
      ]

      cassandra_nodetool_pending = False
      cassandra_cql_pending = False
      spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
      frame = 0

      with Live(Align.center(self.m._build_svc_table(services)), refresh_per_second=10, screen=False) as live:
        while not all(s["ready"] for s in services):
          frame += 1
          for s in services:
            if s["ready"]:
              s["status"] = "✓"
            elif self.m._check_port(s["port"]):
              if s["name"] == "Cassandra":
                if not cassandra_nodetool_pending and not cassandra_cql_pending:
                  cassandra_nodetool_pending = True
                s["status"] = spinner[frame % len(spinner)]
              else:
                s["ready"] = True
                s["status"] = "✓"
            else:
              s["status"] = spinner[frame % len(spinner)]

          if cassandra_nodetool_pending:
            node_un = subprocess.run(
              f"docker exec {os.getenv('CASSANDRA_CONTAINER', 'cassandra')} "
              f"nodetool status 2>&1 | grep -q '^UN'",
              shell=True, capture_output=True
            ).returncode == 0
            if node_un:
              cassandra_nodetool_pending = False
              cassandra_cql_pending = True

          if cassandra_cql_pending:
            cql_ready = subprocess.run(
              f"docker exec {os.getenv('CASSANDRA_CONTAINER', 'cassandra')} "
              f"cqlsh -e 'DESCRIBE KEYSPACES' 2>/dev/null",
              shell=True, capture_output=True
            ).returncode == 0
            if cql_ready:
              for s in services:
                if s["name"] == "Cassandra":
                  s["ready"] = True
                  s["status"] = "✓"
              cassandra_cql_pending = False

          live.update(Align.center(self.m._build_svc_table(services)))
          time.sleep(0.08)

      _status_line = ""
      logging.info('Todos los servicios están listos.')

    def show_service_logs(self, service_name: str):
      '''
      Guarda los logs del servicio en logs/ y luego los abre con less.
      '''        
      log_file = f"logs/docker_{service_name}.log"
      logging.info(f"💾 Actualizando archivo de log: {log_file}")
      dump_cmd = f"docker compose logs --no-color {service_name} > {log_file}"
      subprocess.run(dump_cmd, shell=True, cwd=self.m.project_home)
      
      # Abrimos el archivo que acabamos de crear/actualizar
      cmd = f"less -S +G {log_file}"
      subprocess.run(cmd, shell=True)

    class Database:
      def __init__(self, manager):
        self.m = manager

      def import_distances(self):
        # DB_MODE ('mongo' o 'cassandra') se decide en start_services()
        # y lo resuelve internamente scripts/import_distances.py
        db_mode = os.getenv('DB_MODE', 'cassandra')
        label = "Cassandra" if db_mode == 'cassandra' else "MongoDB"
        logging.info(f"📊 Importando distancias a {label}...")
        return self.m.run_local_script('import_distances.py')

    class Spark:
      def __init__(self, manager):
        self.m = manager
        self.container_name = os.getenv('SPARK_CONTAINER', 'spark')
        self.prediction_log = os.getenv('PREDICTION_LOG', 'logs/flight_prediction_2.13-0.1.jar.log')

      def train_model(self):
        logging.info("🧠 Iniciando entrenamiento Spark MLlib...")
        spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077')
        access_key = os.getenv('MINIO_ROOT_USER', 'admin')
        secret_key = os.getenv('MINIO_ROOT_PASSWORD', 'password')
        cmd = (
          f"docker exec {self.container_name} spark-submit "
          f"--master {spark_master} "
          f"--conf spark.hadoop.fs.s3a.access.key={access_key} "
          f"--conf spark.hadoop.fs.s3a.secret.key={secret_key} "
          f"scripts/train.py"
        )
        
        return self.m._run_command(cmd)

      def predict_delay(self):
        logging.info("🧠 Lanzando predicción en Spark...")
        spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark:7077')
        minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
        access_key = os.getenv('MINIO_ROOT_USER', 'admin')
        secret_key = os.getenv('MINIO_ROOT_PASSWORD', 'password')
        prediction_jar = os.getenv('PREDICTION_JAR', 
          '/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar')
        
        cmd = (
          f"docker exec {self.container_name} spark-submit "
          f"--master {spark_master} "
          f"--deploy-mode cluster "
          f"--conf spark.hadoop.fs.s3a.endpoint={minio_endpoint} "
          f"--conf spark.hadoop.fs.s3a.access.key={access_key} "
          f"--conf spark.hadoop.fs.s3a.secret.key={secret_key} "
          f"--conf spark.hadoop.fs.s3a.path.style.access=true "
          f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
          f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
          f"{prediction_jar}"
        )
        return self.m._run_command(cmd, wait=False)

      def show_prediction_logs(self):
        '''Abre el log del JAR de Scala con less'''
        if os.path.exists(self.prediction_log):
          subprocess.run(f"less -S +G {self.prediction_log}", shell=True)
        else:
          rich.print("[bold red]Archivo de log no encontrado.[/bold red]")
        
    class Kafka:
      def __init__(self, manager):
        self.m = manager
        self.name = os.getenv('KAFKA_CONTAINER', 'kafka')

      def create_topic(self, topic_name):
        logging.info(f"📝 Creando tópico '{topic_name}' en Docker...")
        kafka_local = os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092')
        cmd = (
            f"docker exec {self.name} /opt/kafka/bin/kafka-topics.sh "
            f"--create --bootstrap-server {kafka_local} "
            f"--topic {topic_name} --partitions 1 --replication-factor 1 --if-not-exists"
        )
        return self.m._run_command(cmd)

  class KubernetesMode:
    def __init__(self, manager):
      raise NotImplementedError('Modo Kubernetes no implementado aún.')

def main_docker(db: Literal['mongo', 'cassandra'] = 'mongo'):
  manager = ClusterManager()

  with console.status("[dim]Cleaning previous sessions[/dim]", spinner="dots"):
    subprocess.run('docker compose --profile db_mongo --profile db_cassandra down 2>/dev/null',
      shell=True, cwd=manager.project_home, capture_output=True)

  try:
    manager.docker.start_services(db=db)

    vm_ip = 'localhost'
    try:
      vm_ip = subprocess.run(
        ['hostname', '-I'], capture_output=True, text=True, check=True
      ).stdout.strip().split()[0]
    except Exception:
      pass

    with console.status("[dim]Creating MinIO bucket[/dim]", spinner="dots"):
      result = manager.run_local_script('create_bucket.py')
    if result is None:
      logging.error("Fallo en create_bucket.py. Abortando.")
      return
    set_status("Bucket created \u2713")

    with console.status("[dim]Downloading data[/dim]", spinner="dots"):
      result = manager.run_local_script('download_data.py')
    if result is None:
      logging.error("Fallo en download_data.py. Abortando.")
      return
    set_status("Data downloaded \u2713")

    with console.status("[dim]Importing distances to Cassandra[/dim]", spinner="dots"):
      result = manager.docker.db.import_distances()
    if result is None:
      logging.error("Fallo en import_distances. Abortando.")
      return
    set_status("Distances imported \u2713")

    with console.status("[dim]Creating Kafka topics[/dim]", spinner="dots"):
      result = manager.docker.kafka.create_topic(os.getenv('KAFKA_TOPIC', 'flight-delay-ml-request'))
      if result is not None:
        result = manager.docker.kafka.create_topic(os.getenv('KAFKA_RESPONSE_TOPIC', 'flight-delay-ml-response'))
    if result is None:
      logging.error("Fallo en create_topic. Abortando.")
      return
    set_status("Kafka topics created \u2713")

    with console.status("[dim]Starting Spark streaming job[/dim]", spinner="dots"):
      result = manager.docker.spark.predict_delay()
    if result is None:
      logging.error("Fallo en predict_delay. Abortando.")
      return
    set_status("Spark streaming running \u2713")

    flk_port = os.getenv('FLASK_PORT', '5001')
    manager._wait_for_http(f"http://{vm_ip}:{flk_port}/flights/delays/predict_kafka", timeout=30)

    set_status(f"API ready: http://{vm_ip}:{flk_port}/flights/delays/predict_kafka")
    set_status("")

    rich.print("\n[bold]Cluster ready[/bold]  ·  [dim]Press Ctrl+C to shutdown[/dim]")
    rich.print("[dim]" + "─" * (console.width-2) + "[/dim]")

    while True:
      main_choice = questionary.select(
        "ibdn@cluster",
        choices=[ "📋 Menú de logs" ]
      ).ask()

      if main_choice is None:
        break

      if main_choice == "📋 Menú de logs":
        db_label = "MongoDB" if db == 'mongo' else "Cassandra"
        db_service = "mongodb" if db == 'mongo' else "cassandra"
        log_services = [
          "Kafka",
          "Spark (Master)",
          "Spark Worker",
          "Predicción Spark MLlib",
          db_label,
          "Flask",
          "MinIO",
          "← Volver",
        ]
        rich.print(f"\n   [dim]Servicios: Kafka / Spark (Master) / Spark Worker /\n"
                   f"   Predicción MLlib / {db_label} / Flask / MinIO[/dim]")
        while True:
          log_choice = questionary.autocomplete(
            "📋 Filtrar:",
            choices=log_services,
            complete_style=CompleteStyle.READLINE_LIKE
          ).ask()

          if log_choice is None or log_choice == "← Volver":
            break

          if log_choice == "Kafka":
            manager.docker.show_service_logs("kafka")
          elif log_choice == "Spark (Master)":
            manager.docker.show_service_logs("spark")
          elif log_choice == "Spark Worker":
            manager.docker.show_service_logs("spark-worker")
          elif log_choice == "Predicción Spark MLlib":
            manager.docker.spark.show_prediction_logs()
          elif log_choice == db_label:
            manager.docker.show_service_logs(db_service)
          elif log_choice == "Flask":
            manager.docker.show_service_logs("flask")
          elif log_choice == "MinIO":
            manager.docker.show_service_logs("minio")

  except KeyboardInterrupt:
    set_status("Shutting down cluster...")
  finally:
    with console.status("[dim]Stopping containers[/dim]", spinner="dots"):
      manager._run_command(
        'docker compose --profile db_mongo --profile db_cassandra down',
        cwd=manager.project_home)
    _status_line = ""
    set_status("Containers stopped. Goodbye!")

def main_kubernetes(db: Literal['mongo', 'cassandra']):
  raise NotImplementedError('Función main_kubernetes no implementada aún.')

if __name__ == '__main__':
  os.system('clear')
  print()
  title_panel = Panel(
      Align.center(Text.assemble(
          ("\nCluster Orchestrator", "bold white"),
          (" · ", "dim white"),
          ("v2.0\n", "bold cyan"),
          ("Flight Delay Prediction\n", "dim white"),
      )),
      border_style="bright_blue",
      title="[bold bright_blue]✈ IBDN ✈[/bold bright_blue]",
      title_align="center",
      padding=(0, 2),
      box=box.HEAVY,
  )
  console.print(title_panel)
  console.print()

  infra = questionary.select(
      "¿Qué infraestructura deseas usar?",
      choices=[
          "Docker",
          "Kubernetes"
      ],
      default="Docker"
  ).ask()

  db_choice = questionary.select(
      "¿Qué base de datos quieres levantar?",
      choices=[
          "MongoDB",
          "Cassandra"
      ],
      default="Cassandra"
  ).ask()

  # Clear 2 lines of questions from terminal
  sys.stdout.write("\033[1A\033[2K\033[1A\033[2K")
  sys.stdout.flush()

  infra_mode = "docker" if "Docker" in infra else "kubernetes"
  db = "mongo" if "MongoDB" in db_choice else "cassandra"
  db_label = "MongoDB" if db == 'mongo' else "Cassandra"

  console.print(Align.center(Text.assemble(
      ("⚙️ ", "bold yellow"),
      ("Docker", "bold white"),
      (" + ", "dim white"),
      (db_label, "bold cyan"),
  )))

  if infra_mode == "docker":
    main_docker(db=db)
  elif infra_mode == "kubernetes":
    main_kubernetes(db=db)