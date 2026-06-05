import os
import subprocess
import logging
from textwrap import dedent

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def import_to_mongodb(project_home, src_file):
    db_name = os.getenv('MONGODB_DATABASE', 'agile_data_science')
    collection_name = os.getenv('MONGODB_DISTANCES_COLLECTION', 'origin_dest_distances')
    mongo_container = os.getenv('MONGODB_CONTAINER', 'mongodb')

    if not os.path.exists(src_file):
        logging.error(f"❌ No se encuentra el archivo de datos: {src_file}")
        return

    logging.info("📊 Iniciando importación directa a MongoDB...")

    import_cmd = (
        f"docker exec -i {mongo_container} mongoimport "
        f"--db {db_name} --collection {collection_name} "
        f"--drop < {src_file}"
    )

    try:
        subprocess.run(import_cmd, shell=True, check=True)
        logging.info("✅ Datos enviados a MongoDB.")

        index_cmd = (
            f'docker exec -i {mongo_container} mongosh {db_name} --eval '
            f'"db.{collection_name}.createIndex({{Origin: 1, Dest: 1}})"'
        )
        subprocess.run(index_cmd, shell=True, check=True)
        logging.info("✅ Índice creado correctamente.")

    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Error durante la operación: {e}")

def import_to_cassandra(project_home, src_file):
    keyspace = os.getenv('MONGODB_DATABASE', 'agile_data_science')
    table = os.getenv('MONGODB_DISTANCES_COLLECTION', 'origin_dest_distances')
    cassandra_host = os.getenv('CASSANDRA_CONTAINER', 'cassandra')

    if not os.path.exists(src_file):
        logging.error(f"❌ No se encuentra el archivo de datos: {src_file}")
        return

    logging.info("📊 Iniciando importación a Cassandra via Docker exec...")

    # 1. Crear keyspace via cqlsh (evita cassandra-driver local)
    r = subprocess.run(
        ['docker', 'exec', 'cassandra', 'cqlsh', '-e',
         f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
         f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"],
        capture_output=True, text=True)
    if r.returncode != 0:
        logging.error(f"Error creando keyspace: {r.stderr.strip()}")
        return

    # 1b. Crear tabla de predicciones
    subprocess.run(
        ['docker', 'exec', 'cassandra', 'cqlsh', '-e',
         "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response ("
         "uuid text PRIMARY KEY, prediction int, origin text, dest text, dep_delay double, "
         "carrier text, flight_date text, flight_num text, distance double, route text, "
         "day_of_year int, day_of_month int, day_of_week int, timestamp text)"],
        capture_output=True, text=True)

    # 2. Crear tabla e importar datos con Python dentro del contenedor flask
    #    (el contenedor flask tiene cassandra-driver instalado)
    script = dedent(f"""\
    import json, sys
    from cassandra.cluster import Cluster
    cluster = Cluster(['{cassandra_host}'], port=9042)
    session = cluster.connect()
    session.set_keyspace('{keyspace}')
    session.execute('CREATE TABLE IF NOT EXISTS {table} (origin text, dest text, distance double, PRIMARY KEY (origin, dest))')
    session.execute('TRUNCATE {table}')
    inserted = 0
    for line in sys.stdin:
        row = json.loads(line)
        session.execute(
            'INSERT INTO {table} (origin, dest, distance) VALUES (%(o)s, %(d)s, %(dist)s)',
            {{'o': row['Origin'], 'd': row['Dest'], 'dist': row['Distance']}}
        )
        inserted += 1
    cluster.shutdown()
    print(f'{{inserted}} registros importados a Cassandra.')
    """)

    with open(src_file) as f:
        r = subprocess.run(
            ['docker', 'exec', '-i', 'flask', 'python3', '-c', script],
            stdin=f, capture_output=True, text=True)

    if r.returncode == 0:
        logging.info(f"✅ {r.stdout.strip()}")
    else:
        logging.error(f"Error importando datos: {r.stderr.strip()}")

def main():
    project_home = os.path.expanduser(os.getenv('PROJECT_HOME', '/app'))
    src_file = os.path.join(project_home, os.getenv('DISTANCES_FILE', 'data/origin_dest_distances.jsonl'))
    db_mode = os.getenv('DB_MODE', 'cassandra')

    if db_mode == 'cassandra':
        import_to_cassandra(project_home, src_file)
    else:
        import_to_mongodb(project_home, src_file)

if __name__ == "__main__":
    main()
