# Instrucciones para ejecutar con Google Cloud

Se entiende que ya se han seguido los pasos de el [README](../README.md) para configurar el proyecto. Aquí se detallan los pasos específicos para ejecutar la aplicación usando Docker en Google Cloud.

En realidad el proceso es muy similar al de Docker, por lo que se solicita al lector que revise esa sección y posteriormente revise esta guía, que contendrá exclusivamente las diferencias con el proceso de Docker.

## Desplegar

Para desplegar la aplicación, puedes usar el comando `predict` con el argumento `gcloud` y elegir la base de datos que quieres usar (Cassandra o MongoDB):

```shell
predict gcloud --db cassandra   # POR DEFECTO
predict gcloud --db mongo
```

> [!WARNING]
> Actualmente, el despliegue se esta saltando el bloqueo del pipeline para acceder a la app. Cuando acceda a la app, es posible que las funciones no funcionen correctamente. Se recomienda revisar los logs esperando a que todo este listo antes de tratar de usar la app (no va a funcionar).

## Diferencias en la interfaz
En la vista de modelos de Google Cloud, se puede ver un nuevo botón:

![GCloud Models](../images/gcloud-models.png)

Este botón permite transferir los modelos entrenados desde tu máquina local hasta Google Cloud. Es especialmente útil si vienes de ejecutar esa sección y quieres aprovechar los modelos o ahorrar tiempo de entrenamiento.