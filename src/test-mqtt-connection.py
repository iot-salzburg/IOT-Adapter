#!/usr/bin/env python3
#  -*- coding: utf-8 -*-

"""src.py: This module fetches heterogenious data from the Iot-Lab's MQTT_BORKER
(3D printer data previously bundled on node-red), converts it into the canonical dataformat as
specified in SensorThings and sends it in the Kafka message Bus.
If MQTT doesn't work, make sure that
1) you are listening on port 1883 (cmd: netstat -a)
2) mosquitto is running (cmd path/to/mosquitto mosquitto)
3) you can listen to the incoming MQTT data on chrome's MQTT Lens."""

import os
import sys
import inspect
import logging
import json
import pytz
from datetime import datetime

import paho.mqtt.client as mqtt

MQTT_BROKER = "broker.blusensor.com"
MQTT_PORT = 7883  # client port
# MQTT_PORT = 8883  # client port with TLS
GATEWAY_ID = "660D999D84FB5F40"
temp_sensor = "246F28432CB6"
air_sensor = "246F28432BB6"
def_topics = ",".join(["iot/blusensor/v1/gateway/246F28432BB6/thing/24:6F:28:43:2B:B6/data",
                       "iot/blusensor/v1/gateway/246F28432CB6/thing/24:6F:28:43:2C:B6/data"])
SUBSCRIBED_TOPICS = os.environ.get("MQTT_SUBSCRIBED_TOPICS", def_topics).split(",")
# SUBSCRIBED_TOPICS = ["#"]

logger = logging.getLogger("bluSensor-Adapter_Logger")
logger.setLevel(logging.DEBUG)
logging.basicConfig()


def define_mqtt_statemachine():
    """
    Setting up MQTT client and define function on mqtt events.
    :return:
    """
    # The protocol must be specified in python!
    client = mqtt.Client(protocol=mqtt.MQTTv31)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    logger.info("Connection to {} on port {} established at {} UTC".format(MQTT_BROKER, MQTT_PORT,
                                                                           datetime.utcnow().isoformat()))

    client.loop_forever()


def on_connect(client, userdata, flags, rc):
    """Report if connection to MQTT_BROKER is established
    and subscribe to all topics. MQTT subroutine"""
    logger.info("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for mqtt_topic in SUBSCRIBED_TOPICS:
        logger.debug("subscribe to topic '{}'".format(mqtt_topic))
        client.subscribe(mqtt_topic)


def on_disconnect(client, userdata, rc):
    """Reporting if connection to MQTT_BROKER is lost. MQTT subroutine"""
    logger.warning("Disconnect, reason: " + str(rc), level="warning")
    logger.warning("Disconnect, reason: " + str(client), level="warning")


def on_message(client, userdata, msg):
    """
    Action on received message:
    Raw message will be parsed to the canonical data format specified with SensorThings
    and published on the Kafka message bus via the kafka producer.
    Nothing to return, because kafka message sending is called here.
    :param client: not used, but part of routine.
    :param userdata: not used, but part of routine.
    :param msg: Incoming raw MQTT message
    :return:
    """

    # payload: print(json.dumps(data, indent=2))
    # {
    #     "mac": "24:6F:28:43:2C:B6",
    #     "gwid": "246F28432CB6",
    #     "ts_unix": 1569314556,
    #     "ts_iso": "2019-09-24 10:42:36",
    #     "name": "bluSensor AIQ",
    #     "type": 10,
    #     "lat": "0.000000",
    #     "lon": "0.000000",
    #     "hum": 26.8,
    #     "tem": 35.3,
    #     "dew": 13.4,
    #     "co2": 400,
    #     "tvoc": 102
    # }
    # {
    #     "mac": "24:6F:28:43:2B:B6",
    #     "gwid": "246F28432BB6",
    #     "ts_unix": 1569314549,
    #     "ts_iso": "2019-09-24 10:42:29",
    #     "name": "bluSensor APM",
    #     "type": 15,
    #     "lat": "0.000000",
    #     "lon": "0.000000",
    #     "pm1": 1.7,
    #     "pm2": 1.8,
    #     "pm4": 1.8,
    #     "pm10": 1.8
    # }
    logger.info("Received new data with topic: {}".format(msg.topic))
    data = json.loads(msg.payload.decode("utf-8"))
    # logger.debug(json.dumps(data, indent=2))
    try:
        if msg.topic not in MQTT_TOPICS:
            MQTT_TOPICS.append(msg.topic)
            with open(topics_list_file, "w") as topics:
                json.dump({"topics": sorted(MQTT_TOPICS)}, topics, indent=4, sort_keys=True)
                logger.info("Found new mqtt topic: {} and saved it to file".format(msg.topic))

        if data.get("boot"):
            logger.info("Found booted device with mac '{}'".format(data.get("mac")))
            return

        message = get_basic_message(data)
        if data.get("mac") == "24:6F:28:43:2C:B6":
            logger.debug("  sending air data")
            sense_map = {"hum": "humidity",
                         "tem": "temperature",
                         "dew": "dew point",
                         "co2": "CO2 concentration",
                         "tvoc": "VOC concentration"}
        elif data.get("mac") == "24:6F:28:43:2B:B6":
            logger.debug("  sending particle data")
            sense_map = {"pm1": "particle-conc. 1pm",
                         "pm2": "particle-conc. 2.5pm",
                         "pm4": "particle-conc. 4pm",
                         "pm10": "particle-conc. 10pm"}
        else:
            logger.warning("Unknown data-type: topic: {}\npayload: {}".format(msg.topic, msg.payload))
            return

        for quant, q_name in sense_map.items():
            message["Datastream"]["name"] = data.get("name") + " " + q_name
            message["result"] = float(data.get(quant))
            logger.info("{} = {}".format(message["Datastream"]["name"], message["result"]))
            # logger.info(json.dumps(message, indent=2))

    except Exception as e:
        logger.warning("Unexpected exception occured: {}".format(e))
        logger.warning(json.dumps(data, indent=2))



def get_basic_message(data):
    message = dict()
    message["Datastream"] = dict()
    try:
        message["phenomenonTime"] = datetime.utcfromtimestamp(data["ts_unix"]).replace(tzinfo=pytz.UTC).isoformat()
    except:
        logger.warning("couldn't parse datetime: {}".format(data))
        message["phenomenonTime"] = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
    message["resultTime"] = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
    return message


if __name__ == '__main__':
    logger.info("Started bluSensor Adapter")

    # Get dirname from inspect module
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    dirname = os.path.dirname(os.path.abspath(filename))
    topics_list_file = os.path.join(dirname, "topics_list.json")
    with open(topics_list_file) as topics_file:
        MQTT_TOPICS = json.load(topics_file).get("topics", list())

    define_mqtt_statemachine()
