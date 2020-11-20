#!/usr/bin/env python3
#  -*- coding: utf-8 -*-

"""src.py: This module fetches heterogeneous data from the Iot-Lab's MQTT_BORKER
(3D printer data previously bundled on node-red), converts it into the canonical data format as
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

# confluent_kafka is based on librdkafka, details in requirements.txt
try:
    from .panta_rhei.client.digital_twin_client import DigitalTwinClient
except ImportError:
    from panta_rhei.client.digital_twin_client import DigitalTwinClient


__author__ = "Salzburg Research"
__version__ = "2.3"
__date__ = "20 November 2020"
__email__ = "christoph.schranz@salzburgresearch.at"
__status__ = "Development"

MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.48.71")
SUBSCRIBED_TOPICS = os.environ.get("MQTT_SUBSCRIBED_TOPICS", "prusa3d/#,sensorpi/#,octoprint/#").split(",")

# Panta Rhei configuration
config = {"client_name": os.environ.get("CLIENT_NAME", "mqtt-adapter"),
          "system": os.environ.get("SYSTEM_NAME", "test-topic"),  # "at.srfg.iot.dtz" in docker-compose env
          "gost_servers": os.environ.get("SENSORTHINGS_HOST", "192.168.48.71:8082"),
          "kafka_bootstrap_servers": os.environ.get("BOOTSTRAP_SERVERS",
                                                    "192.168.48.71:9092,192.168.48.71:9093,192.168.48.71:9094")
          }
# load files relative to this file
dirname = os.path.dirname(os.path.abspath(__file__))
INSTANCES = os.path.join(dirname, "instances.json")



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

    client.connect(MQTT_BROKER, 1883, 60)
    logger.info("Connection to {} on port {} established at {} UTC".format(MQTT_BROKER, 1883,
                                                                           datetime.utcnow().isoformat()))
    pr_client.produce("logging", "Connection to {} on port {} established".format(MQTT_BROKER, 1883))

    client.loop_forever()


def on_connect(client, userdata, flags, rc):
    """Report if connection to MQTT_BROKER is established
    and subscribe to all topics. MQTT subroutine"""
    logger.info("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for mqtt_topic in SUBSCRIBED_TOPICS:
        client.subscribe(mqtt_topic)


def on_disconnect(client, userdata, rc):
    """Reporting if connection to MQTT_BROKER is lost. MQTT subroutine"""
    logger.warning("Disconnect, reason: " + str(rc), level="warning")
    logger.warning("Disconnect, reason: " + str(client), level="warning")
    pr_client.produce("logging", "Disconnect, reason: " + str(client))
    pr_client.produce("logging", "Disconnect, reason: " + str(rc))


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
    print("Received new data with topic: {} and values: {}".format(msg.topic, msg.payload))

    # kafka_logger("New MQTT message: {}, {}".format(msg.topic, "-"), level="debug")  # msg.payload))
    if msg.topic not in MQTT_TOPICS:
        MQTT_TOPICS.append(msg.topic)
        with open(topics_list_file, "w") as topics:
            json.dump({"topics": sorted(MQTT_TOPICS)}, topics, indent=4, sort_keys=True)
            logger.info("Found new mqtt topic: {} and saved it to file".format(msg.topic))
            pr_client.produce("logging", "Found new mqtt topic: {} and saved it to file".format(msg.topic))

    if msg.topic.startswith("testtopic"):
        message = dict()
        message["Datastream"] = msg.topic.replace("/", ".")
        message["result"] = float(msg.payload)
        message["phenomenonTime"] = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
        message["resultTime"] = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()

    elif msg.topic.startswith("octoprint/temperature"):
        # messages = parse_octoprint_temperature(msg)
        pass
    elif msg.topic.startswith("prusa3d/temperature"):
        send_prusa3d_temperature(msg)
    elif msg.topic.startswith("prusa3d/progress"):
        send_prusa3d_progress(msg)
    elif msg.topic.startswith("prusa3d/mqtt"):
        send_prusa3d_mqtt(msg)
    elif msg.topic.startswith("prusa3d/event"):
        send_prusa3d_event(msg)
    elif msg.topic.startswith("sensorpi/"):
        send_sensorpi_message(msg)
    else:
        logger.warning("Found unparsed message: {}: {}".format(msg.topic, msg.payload))


def send_sensorpi_message(msg):
    # That mapping must fit from mqtt topic name to gost_instances datastream name
    sensorpi_mapping = dict({
        "sensorpi/temperature": "temperature",
        "sensorpi/humidity": "humidity",
        "sensorpi/current0": "panda_current",
        "sensorpi/current1": "prusa_current",
        "sensorpi/current2": "pixtend_current",
        "sensorpi/current3": "sigmatek_current"
    })
    logger.debug("Sending SensorPi Data to Panta Rhei")
    # print("Quantity: {}, payload: {}".format(sensorpi_mapping[msg.topic], msg.payload.decode("utf-8")))
    pr_client.produce(quantity=sensorpi_mapping[msg.topic], result=msg.payload.decode("utf-8"))


def send_prusa3d_temperature(msg):
    payload = json.loads(msg.payload.decode("utf-8"))

    for direction in ["target", "actual"]:
        if direction in payload.keys():
            if msg.topic == "prusa3d/temperature/bed":
                quantity = "prusa3d.bed.temp.{}".format(direction)
            elif msg.topic == "prusa3d/temperature/tool0":
                quantity = "prusa3d.tool0.temp.{}".format(direction)
            else:
                logger.warning("Octoprint quantity not implemented.")
                continue
            # print("Quantity: {}, payload: {}".format(quantity, payload[direction]))
            pr_client.produce(quantity=quantity, result=payload[direction], timestamp=payload["_timestamp"])
        else:
            logger.warning("Octoprint payload direction not implemented.")


def send_prusa3d_progress(msg):
    payload = json.loads(msg.payload.decode("utf-8"))
    # print("Quantity: {}, payload: {}".format("prusa3d.progress.status", payload))
    pr_client.produce(quantity="prusa3d.progress.status", result=msg.payload, timestamp=payload["_timestamp"])


def send_prusa3d_mqtt(msg):
    payload = msg.payload.decode("utf-8")
    # print("Quantity: {}, payload: {}".format("prusa3d.mqtt.status", payload))
    pr_client.produce(quantity="prusa3d.mqtt.status", result=msg.payload, timestamp=payload["_timestamp"])


def send_prusa3d_event(msg):
    # Skip ZChange information as they occur to frequently
    if msg.topic == "prusa3d/event/ZChange":
        return None
    payload = json.loads(msg.payload.decode("utf-8"))
    if payload.get("_event") in ["CaptureStart", "CaptureDone"]:
        return None

    # print("Quantity: {}, payload: {}".format("prusa3d.event.status", payload))
    pr_client.produce(quantity="prusa3d.event.status", result=msg.payload, timestamp=payload["_timestamp"])


if __name__ == '__main__':
    logger = logging.getLogger("MQTT-Adapter_Logger")
    logger.setLevel(logging.INFO)

    logging.basicConfig()
    logger.info("Started MQTT Adapter")

    # Get dirname from inspect module
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    dirname = os.path.dirname(os.path.abspath(filename))
    topics_list_file = os.path.join(dirname, "topics_list.json")
    with open(topics_list_file) as topics_file:
        MQTT_TOPICS = json.load(topics_file)["topics"]

    pr_client = DigitalTwinClient(**config)
    pr_client.logger.info("Main: Starting client.")
    pr_client.register(instance_file=INSTANCES)

    logger.info("Configured the Panta Rhei Client")
    define_mqtt_statemachine()
