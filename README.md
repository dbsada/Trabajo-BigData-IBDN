# Resolución de la Práctica Creativa

Este repositorio contiene la resolución de la [práctica creativa](https://github.com/Big-Data-ETSIT/practica_creativa) de la asignatura Ingeniería Big Data en la Nube.

## Entorno de trabajo

La práctica se realizó en una máquina virtual basada en la imagen oficial de Ubuntu Server 22.04.5 ARM64 ([descargar aquí](https://cdimage.ubuntu.com/releases/jammy/release/ubuntu-22.04.5-live-server-arm64.iso)), desplegada con UTM. El acceso y desarrollo se llevaron a cabo a través de SSH, utilizando la extensión de VS Code.

#### Instalar Docker y Docker Compose
```sh
sudo apt update
sudo apt install -y ca-certificates curl gnupg

sudo install -m 0755 -d /etc/apt/keyrings

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

sudo chmod a+r /etc/apt/keyrings/docker.gpg

sudo install -m 0755 -d /etc/apt/keyrings

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu \
$(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker.io docker-compose-plugin
```

#### Instalar Python
```sh
sudo apt install -y python3 python3-pip
sudo apt install -y python3-venv
```


docker run -it --rm \
  -v $(pwd):/app \
  -w /app/flight_prediction \
  eclipse-temurin:17-jdk \
  sh -c "curl -sL https://github.com/sbt/sbt/releases/download/v1.9.7/sbt-1.9.7.tgz | tar -xz -C /usr/local && /usr/local/sbt/bin/sbt clean package"