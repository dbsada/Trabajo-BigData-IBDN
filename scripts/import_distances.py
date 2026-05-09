import os
import subprocess
import json
import logging

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
    cassandra_host = os.getenv('CASSANDRA_CONTAINER', 'localhost')
    cassandra_port = int(os.getenv('CASSANDRA_PORT', '9042'))

    if not os.path.exists(src_file):
        logging.error(f"❌ No se encuentra el archivo de datos: {src_file}")
        return

    logging.info("📊 Iniciando importación a Cassandra...")

    from cassandra.cluster import Cluster
    from cassandra.cluster import NoHostAvailable
    import time

    session = None
    for attempt in range(10):
        try:
            cluster = Cluster([cassandra_host], port=cassandra_port)
            session = cluster.connect()
            break
        except NoHostAvailable:
            if attempt < 9:
                logging.info(f"⏳ Esperando a Cassandra (intento {attempt + 1}/10)...")
                time.sleep(5)
            else:
                raise

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(keyspace)

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            origin text,
            dest text,
            distance double,
            PRIMARY KEY (origin, dest)
        )
    """)

    session.execute(f"TRUNCATE {table}")

    inserted = 0
    with open(src_file) as f:
        for line in f:
            row = json.loads(line)
            session.execute(
                f"INSERT INTO {table} (origin, dest, distance) VALUES (%s, %s, %s)",
                (row['Origin'], row['Dest'], row['Distance'])
            )
            inserted += 1

    cluster.shutdown()
    logging.info(f"✅ {inserted} registros importados a Cassandra.")

def main():
    project_home = os.path.expanduser(os.getenv('PROJECT_HOME', '~/ibdn'))
    src_file = os.path.join(project_home, os.getenv('DISTANCES_FILE', 'data/origin_dest_distances.jsonl'))
    db_mode = os.getenv('DB_MODE', 'mongo')

    if db_mode == 'cassandra':
        import_to_cassandra(project_home, src_file)
    else:
        import_to_mongodb(project_home, src_file)

if __name__ == "__main__":
    main()
