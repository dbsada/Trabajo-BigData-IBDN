import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    project_home = os.path.expanduser(os.getenv('PROJECT_HOME', '~/ibdn'))
    src_file = os.path.join(project_home, os.getenv('DISTANCES_FILE', 'data/origin_dest_distances.jsonl'))
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

if __name__ == "__main__":
    main()