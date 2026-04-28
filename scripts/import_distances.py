import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    project_home = os.path.expanduser("~/ibdn")
    src_file = os.path.join(project_home, "data/origin_dest_distances.jsonl")
    
    if not os.path.exists(src_file):
        logging.error(f"❌ No se encuentra el archivo de datos: {src_file}")
        return

    logging.info("📊 Iniciando importación directa a MongoDB...")

    # Usamos cat para leer el archivo y lo pasamos por tubería a docker exec
    # El '-' al final de mongoimport le dice que lea de la entrada estándar (stdin)
    import_cmd = (
        f"docker exec -i mongodb mongoimport "
        f"--db agile_data_science --collection origin_dest_distances "
        f"--drop < {src_file}"
    )

    try:
        # 1. Importar datos
        subprocess.run(import_cmd, shell=True, check=True)
        logging.info("✅ Datos enviados a MongoDB.")

        # 2. Crear el índice
        index_cmd = (
            'docker exec -i mongodb mongosh agile_data_science --eval '
            '"db.origin_dest_distances.createIndex({Origin: 1, Dest: 1})"'
        )
        subprocess.run(index_cmd, shell=True, check=True)
        logging.info("✅ Índice creado correctamente.")

    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Error durante la operación: {e}")

if __name__ == "__main__":
    main()