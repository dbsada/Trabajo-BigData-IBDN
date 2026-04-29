import os
import subprocess
import logging
import socket
import time
from enum import Enum
import rich
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.console import Console
import questionary
from typing import Literal
import requests

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/orchestrator.log"), # Al archivo
        logging.StreamHandler()                      # A la pantalla
    ]
)

class ClusterManager:
  def __init__(self):
    self.home = os.path.expanduser('~')
    self.project_home = os.path.join(self.home, 'ibdn') 
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
    
  class DockerMode:
    def __init__(self, manager):
      self.m = manager
      self.mongo = self.Mongo(manager)
      self.cassandra = self.Cassandra(manager)
      self.spark = self.Spark(manager)
      self.kafka = self.Kafka(manager)
    
    def start_services(self, db: Literal['mongo', 'cassandra']):
      logging.info('🚀 Iniciando servicios con Docker Compose...')
      self.m._run_command(f'docker compose --profile db_{db} up -d', cwd=self.m.project_home)
      
      self.m._wait_for_port(9092)
      logging.info('Kafka está listo.')

      if db == 'mongo':
        port = 27017
        self.m._wait_for_port(port)
        logging.info('MongoDB está listo.')
      elif db == 'cassandra':
        port = 9042
        self.m._wait_for_port(port)
        logging.info('Cassandra está listo.')
      else:
        logging.error(f'Base de datos no soportada: {db}')
        raise ValueError(f'Base de datos no soportada: {db}')

      self.m._wait_for_port(8080)
      logging.info('Spark Master está listo.')
      
      self.m._wait_for_port(5001)
      logging.info('Servicio de API Flask está listo.')

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

    class Mongo:
      def __init__(self, manager):
        self.m = manager

      def import_distances(self):
        logging.info("📊 Importando distancias a MongoDB...")
        return self.m.run_local_script('import_distances.py')

    class Cassandra:
      def __init__(self, manager):
        self.m = manager

      def import_distances(self):
        logging.info("📊 Importando distancias a Cassandra...")
        raise NotImplementedError('Importación a Cassandra no implementada aún.')

    class Spark:
      def __init__(self, manager):
        self.m = manager
        self.container_name = 'spark'
        self.prediction_log = "logs/flight_prediction_2.13-0.1.jar.log"

      def train_model(self):
        logging.info("🧠 Iniciando entrenamiento Spark MLlib...")
        cmd = f"docker exec -it {self.container_name} python3 scripts/train.py ."
        
        return self.m._run_command(cmd)

      def predict_delay(self):
        logging.info("🧠 Lanzando predicción en Spark...")
        cmd = (
          "docker exec spark spark-submit " 
          "--packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 "
          "--master spark://spark:7077 "
          "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
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
        self.name = 'kafka'

      def create_topic(self, topic_name):
        logging.info(f"📝 Creando tópico '{topic_name}' en Docker...")
        cmd = (
            f"docker exec {self.name} /opt/kafka/bin/kafka-topics.sh "
            f"--create --bootstrap-server localhost:9092 "
            f"--topic {topic_name} --partitions 1 --replication-factor 1 --if-not-exists"
        )
        return self.m._run_command(cmd)

  class KubernetesMode:
    def __init__(self, manager):
      raise NotImplementedError('Modo Kubernetes no implementado aún.')

def main_docker(db: Literal['mongo', 'cassandra'] = 'mongo'):
  manager = ClusterManager()

  manager.run_local_script('download_data.py')
  
  manager.docker.start_services(db=db)

  if db == 'mongo':
    manager.docker.mongo.import_distances()
  elif db == 'cassandra':
    manager.docker.cassandra.import_distances()
  
  manager.docker.kafka.create_topic("flight-delay-ml-request")
  # manager.docker.spark.train_model()
  manager.docker.spark.predict_delay()

  manager._wait_for_http("http://localhost:5001/flights/delays/predict_kafka", timeout=30)

  rich.print("\nAccede a la API de predicciones en: [bold blue]http://localhost:5001/flights/delays/predict_kafka[/bold blue]")

  rich.print("\n[bold green]🚀  SERVICIOS:[/bold green]")
  rich.print("─" * 40)
  rich.print("[bold cyan]k[/bold cyan] -> Ver logs de [bold]Kafka[/bold] (Full scroll)")
  rich.print("[bold magenta]s[/bold magenta] -> Ver logs de [bold]Spark[/bold] (Full scroll)")
  rich.print("[bold yellow]w[/bold yellow] -> Ver logs de [bold]Spark Worker[/bold] (Full scroll)")
  rich.print("[bold green]p[/bold green] -> Ver logs de [bold]Predicción Spark MLlib[/bold] (Full scroll)")
  rich.print("[bold blue]m[/bold blue] -> Ver logs de [bold]MongoDB[/bold] (Full scroll)")
  rich.print("[bold blue]f[/bold blue] -> Ver logs de [bold]Flask[/bold] (Full scroll)")
  rich.print("[bold red]ctrl+c[/bold red] -> Detener todo y [bold]Salir[/bold]")
  rich.print("─" * 40)

  try:
    while True:
      choice = input("ibdn@cluster > ").lower().strip()
      
      if choice == 'k':
        manager.docker.show_service_logs("kafka")
      elif choice == 's':
        manager.docker.show_service_logs("spark")
      elif choice == 'w':
        manager.docker.show_service_logs("spark-worker")
      elif choice == 'p':
        manager.docker.spark.show_prediction_logs()
      elif choice == 'm':
        manager.docker.show_service_logs("mongodb")
      elif choice == 'f':
        manager.docker.show_service_logs("flask")
      elif choice == '':
        continue
      else:
        rich.print("[yellow]Opciones válidas: k, s, w, p, m, f [/yellow]")

  except KeyboardInterrupt:
    rich.print("\n[bold red]🛑 Apagando el cluster...[/bold red]")
    manager._run_command('docker compose down', cwd=manager.project_home)
    rich.print("[grey70]Contenedores detenidos. ¡Hasta pronto![/grey70]")

def main_kubernetes(db: Literal['mongo', 'cassandra']):
  raise NotImplementedError('Función main_kubernetes no implementada aún.')

if __name__ == '__main__':
    console = Console()
    
    console.print("[bold cyan]╔════════════════════════════════════════════╗[/bold cyan]")
    console.print("[bold cyan]║      IBDN CLUSTER ORCHESTRATOR v1.0        ║[/bold cyan]")
    console.print("[bold cyan]╚════════════════════════════════════════════╝[/bold cyan]\n")

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
        default="MongoDB"
    ).ask()

    infra_mode = "docker" if "Docker" in infra else "kubernetes"
    db = "mongo" if "MongoDB" in db_choice else "cassandra"

    console.print(f"\n[bold green]⚙️ Configuración seleccionada:[/bold green] [white]{infra_mode} + {db}[/white]\n")

    if infra_mode == "docker":
      main_docker(db=db)
    elif infra_mode == "kubernetes":
      main_kubernetes(db=db)