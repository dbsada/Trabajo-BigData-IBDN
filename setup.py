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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
      log_name = f'{command.split("/")[-1].split(" ")[0]}.log'
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

    def show_service_logs(self, service_name: str):
        '''
        Abre los logs del servicio en un paginador (less) para scroll y búsqueda.
        '''        
        cmd = f"docker compose logs {service_name} | less -S +G"
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

      def train_model(self):
        logging.info("🧠 Iniciando entrenamiento Spark MLlib...")
        cmd = f"docker exec -it {self.container_name} python3 scripts/train.py ."
        
        return self.m._run_command(cmd)

    class Kafka:
      def __init__(self, manager):
        self.m = manager
        self.name = 'kafka'

      def create_topic(self, topic_name):
        logging.info(f"📝 Creando tópico '{topic_name}' en Docker...")
        cmd = (
            f"docker exec {self.name} /opt/kafka/bin/kafka-topics.sh "
            f"--create --bootstrap-server localhost:9093 "
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

  rich.print("\n[bold green]🚀  SERVICIOS:[/bold green]")
  rich.print("─" * 40)
  rich.print("[bold cyan]k[/bold cyan] -> Ver logs de [bold]Kafka[/bold] (Full scroll)")
  rich.print("[bold magenta]s[/bold magenta] -> Ver logs de [bold]Spark[/bold] (Full scroll)")
  rich.print("[bold yellow]m[/bold yellow] -> Ver logs de [bold]MongoDB[/bold] (Full scroll)")
  rich.print("[bold red]ctrl+c[/bold red] -> Detener todo y [bold]Salir[/bold]")
  rich.print("─" * 40)

  try:
    while True:
      choice = input("ibdn@cluster > ").lower().strip()
      
      if choice == 'k':
        manager.docker.show_service_logs("kafka")
      elif choice == 's':
        manager.docker.show_service_logs("spark")
      elif choice == 'm':
        manager.docker.show_service_logs("mongodb")
      elif choice == '':
        continue
      else:
        rich.print("[yellow]Opciones válidas: k, s, m [/yellow]")

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