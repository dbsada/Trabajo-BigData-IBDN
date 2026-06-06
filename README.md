# Trabajo Big Data - IBDN
Este proyecto es el resultado del trabajo realizado para la asignatura de Big Data en el IBDN. El objetivo principal era desarrollar una aplicación de predicción de retrasos de vuelos utilizando diversas tecnologías y herramientas del ecosistema Big Data. Se puede encontrar el código original en: [https://github.com/Big-Data-ETSIT/practica_creativa](https://github.com/Big-Data-ETSIT/practica_creativa).

| Requisito | ¿Obligatorio? | ¿Hecho? |
| :--- | :---: | :---: |
| S3 + Iceberg | ✅ | ✅ |
| Cassandra | ✅ | ✅ |
| Kafka | ✅ | ✅ |
| Modelos en lakehouse | ✅ | ✅ |
| Docker | ✅ | ✅ |
| K8s | ❌ | ✅ |
| Airflow y MLflow | ❌ | ✅ |
| GCloud | ❌ | ✅ |
| Mejoras | ❌ | ✅ |

> Vease la tabla al final del documento[^1] para ver el significado de cada requisito.

## Pasos para configurar el proyecto:

#### 1. Clonar repositorio:

```shell
git clone https://github.com/dbsada/Trabajo-BigData-IBDN.git
cd Trabajo-BigData-IBDN
```

#### 2. Crear entorno virtual e instalar dependencias:
```shell
source setup.sh
```

#### 3. Configurar variables de entorno:
```shell
cp .env.example .env
```
Edita el archivo `.env` con tu configuración. Necesitas cambiar:
- **`PROJECT_HOME`**: Ruta absoluta a tu directorio de proyecto.
- `GCP_PROJECT`: ID de tu proyecto en Google Cloud (si vas a usar Google Cloud).
- `GCP_ZONE`: Zona de Google Cloud donde desplegarás (si vas a usar Google Cloud).

#### 4. Configurar proyecto GCloud (opcional):
Es necesario tener instalado el SDK de Google Cloud y autenticado para desplegar en Google Cloud o GKE. Si no lo tienes, puedes descargarlo e instalarlo desde [aquí](https://cloud.google.com/sdk/docs/install) (también está disponible en [homebrew](https://formulae.brew.sh/cask/gcloud-cli)).

```shell
gcloud auth login
gcloud config set project YOUR_PROJECT_ID # Reemplaza YOUR_PROJECT_ID con tu ID de proyecto en Google Cloud
```

## Iniciar la aplicación:
Se ha creado un CLI personalizado llamado `predict` para facilitar el despliegue de la aplicación de predicción de retrasos de vuelos en diferentes entornos. Este CLI abstrae los detalles de configuración y despliegue, permitiendo ejecutar comandos simples para iniciar la aplicación.

```shell
predict docker   # Para desplegar localmente con Docker Compose
predict gcloud   # Para desplegar en Google Cloud
predict gke      # Para desplegar en Google Kubernetes Engine
```

El comando acepta un argumento `--db` para elegir entre Cassandra o MongoDB como base de datos:

```shell
predict [docker/gcloud/gke] --db cassandra   # POR DEFECTO
predict [docker/gcloud/gke] --db mongo
```

> [!WARNING]
> No ha sido posible comprobar que el funcionamiento con MongoDB es correcto todavía. Será revisado en las próximas horas.

¡La aplicación está lista! Para entender la interfaz, puedes leer:
- [Instrucciones docker](docs/DOCKER.md)
- [Instrucciones gcloud](docs/GCLOUD.md)
- [Instrucciones gke](docs/GKE.md)

Cada uno irá detallando los pasos específicos de cada modo de despliegue, incluyendo capturas de pantalla para facilitar la comprensión.

## Autores:
- [Diego Besada](https://github.com/dbsada)
- [Natalia Corchón](https://github.com/nataliacorchon)


[^1]: Requisitos detallados:
    | Requisito | Puntos | Descripción |
    | :--- | :---: | :--- |
    | S3 + Iceberg | 1 | Los datos de entrenamiento deben ser almacenados en HDFS o S3/Minio usando Iceberg como Data Lakehouse |
    | Cassandra | 1 | Modificar el código necesario para que las distancias sean almacenadas en Cassandra y que sean leídas desde esa BBDD en lugar de MongoDB |
    | Kafka | 1 | Modificar el código para que el resultado de la predicción se escriba en Kafka y se presente en la aplicación; las predicciones también deben ser almacenadas en Cassandra pero la web las debe leer de Kafka usando websockets |
    | Modelos en lakehouse | 1 | El entrenamiento tiene que leer los datos del Lakehouse desplegado en el punto 1 y almacenar los modelos en el mismo Lakehouse |
    | Docker | 1 | Lograr el funcionamiento de la práctica con Docker: Dockerizar cada uno de los servicios y desplegar el escenario completo usando docker-compose |
    | K8s | 3 | Desplegar el escenario completo con todos los cambios solicitados en K8S |
    | Airflow y MLflow | 1 | Entrenar el modelo con Apache Airflow y MLflow en el cluster spark con docker |
    | GCloud | 1 | Desplegar todo el escenario dockerizado con los cambios solicitados en GCloud |
    | Mejoras | 1 | Mejoras a nivel de despliegue, observabilidad, visualización y optimización |