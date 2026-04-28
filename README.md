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

#### Instalar herramientas via SDKMAN 
```sh
sudo apt install zip -y
sudo apt install unzip -y
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
```

```sh
sdk install java 17.0.14-amzn
sdk install spark 4.1.1
sdk install scala 2.13.0
sdk install sbt
```

#### Instalar Kafka
Se instala desde la [página oficial de Apache Kafka](https://kafka.apache.org/community/downloads/). La versión utilizada es la [4.2.0 con Scala 2.13](https://www.apache.org/dyn/closer.lua/kafka/4.2.0/kafka_2.13-4.2.0.tgz?action=download).

```sh
chmod +x /home/ibdn/ibdn/kafka_2.13-4.2.0/bin/*.sh
```

## Dockerizado


## K8s


