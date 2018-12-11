# MQTT-Adapter
Connecting MQTT services with the Messaging System.
This component subscribes to topics from the Internet of Things
protocol MQTT and forwards them to the messaging system which is based on
Apache Kafka.
Therefore both services must be running:
* [MQTT-broker](https://github.com/iot-salzburg/mqtt-adapter)
* [messaging-system](https://github.com/iot-salzburg/messaging-system)


The MQTT Adapter is based on the components:
* Paho-MQTT Messaging Client, [paho.mqtt](https://pypi.python.org/pypi/paho-mqtt/1.3.1) version **1.3.1**
* Kafka Client [librdkafka](https://github.com/geeknam/docker-confluent-python) version **0.11.1**
* Python Kafka module [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) version **0.9.1.2**


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
3.  Clone this repository
4.  Clone the panta rhei client into the `src`-directory:
        
        cd src/
        git clone https://github.com/iot-salzburg/panta_rhei
        cd panta_rhei/
        git checkout client_0v1 

    Now, the client can be imported in in `mqtt-adapter.py` with:
    
    ```python
    from src.panta_rhei.client.panta_rhei_client import PantaRheiClient
    ```
    To use the client in deployed mode, configure `src/panta_rhei/client/config.json` to:
    
    ```json
    {
      "_comment": "Kafka Config",
      "BOOTSTRAP_SERVERS": "192.168.48.81:9092,192.168.48.82:9092,192.168.48.83:9092",
    
      "_comment": "SensorThings Config: check in setup/gost/docker-compose.yml for the settings",
      "GOST_SERVER": "192.168.48.81:8082",
    
      "_comment": "SensorThings Type Mapping: These types must fit with the kafka topic in config.json of the client.",
      "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_TruthObservation": "pr.dtz.metric",
      "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CountObservation": "pr.dtz.metric",
      "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement": "pr.dtz.metric",
      "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation": "pr.dtz.string",
      "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation": "pr.dtz.object",
      "panta-rhei/Logging": "pr.dtz.logging"
    }
    ```

## Deployment

The MQTT-Adapter uses the optionally Sensorthings to semantically describe
the forwarded data. The later consumption of the sensor data with the
suggested [DB-Adapter](https://github.com/iot-salzburg/DB-Adapter/)
works best with a running and feeded [SensorThings](https://github.com/iot-salzburg/panta_rhei/setup/gost)
Server.



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
cd dtz_mqtt-adapter
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
nmcli device show <interfacename> | grep IP4.DNS
```

####  Traceback of non zero code 4 or 128:

Restart service with
```sudo service docker restart```

or add your dns address as described above




