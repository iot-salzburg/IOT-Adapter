# MQTT-Adapter
Connecting MQTT services with the Messaging System.
This component subscribes to topics from the Internet of Things
protocol MQTT and forwards them to the messaging system which is based on
Apache Kafka.
Therefore both services must be running:
* [MQTT-broker](https://github.com/iot-salzburg/dtz_mqtt-adapter) in the same repository.
* [panta-rhei stack](https://github.com/iot-salzburg/panta_rhei)


The MQTT Adapter is based on the components:
* Paho-MQTT Messaging Client, [paho.mqtt](https://pypi.python.org/pypi/paho-mqtt/1.3.1) version **1.4.0**
* Kafka Client [librdkafka](https://github.com/geeknam/docker-confluent-python) version **2.1**
* Python Kafka module [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) 
version **0.11.6**


## Contents

1. [Requirements](#requirements)
2. [Deployment](#deployment)
3. [Configuration](#configuration)
4. [Trouble-Shooting](#trouble-shooting)


## Requirements

1.  Install [Docker](https://www.docker.com/community-edition#/download) version **1.10.0+**
2.  Install [Docker Compose](https://docs.docker.com/compose/install/) version **1.6.0+**
3.  Make sure the [Panta Rhei](https://github.com/iot-salzburg/panta_rhei) stack is running.
    This MQTT-Adaper requires Apache **Kafka**, as well as the GOST **SensorThings** server.
4.  Make sure the MQTT-broker runs. The `dockerfile` is in the repository under `setup-broker`.
5.  Clone the Panta Rhei client into this repository:
        
        
    git clone https://github.com/iot-salzburg/panta_rhei src/panta_rhei > /dev/null 2>&1 || git -C src/panta_rhei/ pull

Now, the client can be imported and used in `mqtt-adapter.py` with:
    
    ```python
    import os, sys
    sys.path.append(os.getcwd())
    from client.digital_twin_client import DigitalTwinClient

    # Set the configs, create a new Digital Twin Instance and register file structure
    config = {"client_name": "mqtt-adapter",
              "system": "eu.srfg.iot-iot4cps-wp5.CarFleet1",
              "gost_servers": "localhost:8084",
              "kafka_bootstrap_servers": "localhost:9092"}
    client = DigitalTwinClient(**config)
    client.register_existing(mappings_file=MAPPINGS)
    # client.register_new(instance_file=INSTANCES)  # Registering of new instances should be outsourced to the platform
    ```

## Deployment

The MQTT-Adapter uses the optionally Sensorthings to semantically describe
the forwarded data. The later consumption of the sensor data with the
suggested [DB-Adapter](https://github.com/iot-salzburg/DB-Adapter/)
works best with a running and feeded [SensorThings](https://github.com/iot-salzburg/panta_rhei/setup/gost)
Server.


### Starting the MQTT broker

```bash
cd dtz_mqtt-adapter/setup-broker
sudo docker-compose up --build -d
```

### Creating the Kafka Topics if not already done

If zookeeper is specified by `:2181`, the local zookeeper service will be used. 
It may take some seconds until the new topics are distributed on each zookeeper instance in
a cluster setup.

```bash
/kafka/bin/kafka-topics.sh --zookeeper :2181 --list
/kafka/bin/kafka-topics.sh --zookeeper :2181 --create --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact --config retention.ms=241920000 --topic eu.srfg.iot.dtz.data
/kafka/bin/kafka-topics.sh --zookeeper :2181 --create --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact --config retention.ms=241920000 --topic eu.srfg.iot.dtz.external
/kafka/bin/kafka-topics.sh --zookeeper :2181 --create --partitions 3 --replication-factor 1 --config min.insync.replicas=1 --config cleanup.policy=compact --config retention.ms=241920000 --topic eu.srfg.iot.dtz.logging
/kafka/bin/kafka-topics.sh --zookeeper :2181 --list
```

### Testing
Using `docker-compose`: This depends on the **Panta Rhei Stack** and
configured `instance_file`.

```bash
cd dtz_mqtt-adapter
sudo docker-compose up --build -d
```

The flag `-d` stands for running it in background (detached mode):

Watch the logs with:
```bash
sudo docker-compose logs -f
```


### Deployment in the docker swarm
Using `docker stack`:

If not already done, add a regitry instance to register the image
```bash
sudo docker service create --name registry --publish published=5001,target=5000 registry:2
curl 127.0.0.1:5001/v2/
```
This should output `{}`:


If running with docker-compose works, the stack will start by running:


```bash
git clone https://github.com/iot-salzburg/dtz_mqtt-adapter.git
cd dtz_mqtt-adapter/setup-broker
chmod +x st* sh*
./start_mqtt-broker.sh

cd ../../dtz_mqtt-adapter
chmod +x st* sh*
./start_mqtt-adapter.sh
```


Watch if everything worked fine with:

```bash
./show-adapter-stats.sh
docker service logs -f add-mqtt_adapter
```


## Configuration

Right now, the MQTT-Adapter uses the static `datastreams.json` file to
augment the incoming MQTT messages with metadata stored on the
sensorthings server.

The things, sensors, observations and datastreams stored in the server
were created via REST API. More info and the json content for each posted
instance are in `mqtt-adapter/sensorthings/setUpDatastreams.md`.

In future releases, the registration of instances will be automated to
provide an out-of-the-box solution for industrial IOT.


## Trouble-shooting

#### Can't apt-get update in Dockerfile:
Restart the service

```sudo service docker restart```

or add the file `/etc/docker/daemon.json` with the content:
```
{
    "dns": [your_dns, "8.8.8.8", "8.8.8.4"]
}
```
where `your_dns` can be found with the command:

```bash
nmcli device show [interfacename] | grep IP4.DNS
```

####  Traceback of non zero code 4 or 128:

Restart service with
```sudo service docker restart```

or add your dns address as described above




