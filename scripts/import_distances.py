import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    project_home = os.path.expanduser("~/ibdn")
    
    logging.info("📊 Iniciando importación de distancias a MongoDB...")

    # 1. Comando para importar el JSONL
    # -i: modo interactivo para que Docker pase el archivo
    import_cmd = (
        "docker exec -i mongodb mongoimport "
        "--db agile_data_science "
        "--collection origin_dest_distances "
        "--drop " # Borra si ya existía para no duplicar
        "--file /data/db/mongo_import_tmp.jsonl" # Ruta relativa al contenedor
    )

    # Nota técnica: Para que mongoimport vea el archivo, 
    # lo más fácil es copiarlo temporalmente a la carpeta de datos que ya mapeamos
    src_file = os.path.join(project_home, "data/origin_dest_distances.jsonl")
    dest_tmp = os.path.join(project_home, "data/mongo/mongo_import_tmp.jsonl")
    
    if not os.path.exists(src_file):
        logging.error(f"❌ No se encuentra el archivo: {src_file}")
        return

    # Copiamos el archivo a la carpeta que Mongo sí ve
    os.system(f"cp {src_file} {dest_tmp}")

    # Ejecutamos importación
    subprocess.run(import_cmd, shell=True)

    # 2. Crear el índice (usando mongosh que es el estándar actual)
    index_cmd = (
        'docker exec -i mongodb mongosh agile_data_science --eval '
        '"db.origin_dest_distances.createIndex({Origin: 1, Dest: 1})"'
    )
    subprocess.run(index_cmd, shell=True)

    # Limpiamos el temporal
    os.remove(dest_tmp)
    logging.info("✅ Importación e indexación completadas.")

if __name__ == "__main__":
    main()