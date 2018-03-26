# Kafka to Logstash Adapter with status webserver integrated in the Data Analytics Stack

This component subscribes to topics from the Apache Kafka message broker and forwards them to the Logstash instance of a running ELK Stack. Optionally, the adapter maps IDs with the SensorThings Server in order to provide a plain and intuitive data channel management.

The Kafka Adapter based on the components:
* Kafka Client [librdkafka](https://github.com/geeknam/docker-confluent-python) version **0.11.1**
* Python Kafka module [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) version **0.9.1.2**


## Contents

1. [Requirements](#requirements)
2. [Usage](#usage)
3. [Configuration](#configuration)
4. [Trouble-Shooting](#Trouble-shooting)


## Requirements

1. Install [Docker](https://www.docker.com/community-edition#/download) version **1.10.0+**
2. Install [Docker Compose](https://docs.docker.com/compose/install/) version **1.6.0+**
3. Clone this repository


## Usage

Make sure the `elastic stack` ist running on the same host if you start the DB-Adapter.
Test the `elastic stack` in your browser [http://hostname:5601/status](http://hostname:5601/status).


### Testing
Using `docker-compose`:

```bash
cd /iot-Adapter
sudo docker-compose up --build -d
```

The flag `-d` stands for running it in background (detached mode):


Watch the logs with:

```bash
sudo docker-compose logs -f
```


### Deployment in a docker swarm
Using `docker stack`:

If not already done, add a regitry instance to register the image
```bash
cd /iot-Adapter
sudo docker service create --name registry --publish published=5001,target=5000 registry:2
curl 127.0.0.1:5001/v2/
```
This should output `{}`:


If running with docker-compose works, push the image in order to make the customized image runnable in the stack

```bash
cd ../iot-Adapter
sudo docker-compose build
sudo docker-compose push
```

Actually deploy the service in the stack:
```bash
cd ../IOT-Adapter
sudo docker stack deploy --compose-file docker-compose.yml iot-adapter
```


Watch if everything worked fine with:

```bash
sudo docker service ls
sudo docker stack ps iot-adapter
sudo docker service logs iot-adapter_adapter -f
```



## Trouble-shooting

#### Can't apt-get update in Dockerfile:
Restart the service

```sudo service docker restart```

or add the file `/etc/docker/daemon.json` with the content:
```
{
    "dns": [your_dns, "8.8.8.8"]
}
```
where `your_dns` can be found with the command:

```bash
nmcli device show <interfacename> | grep IP4.DNS
```

####  Traceback of non zero code 4 or 128:

Restart service with
```sudo service docker restart```

or add your dns address as described above




