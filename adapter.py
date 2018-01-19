#!/usr/bin/env python3
#  -*- coding: utf-8 -*-

"""adapter.py: This module fetches heterogenious data from the Iot-Lab's MQTT_BORKER
(3D printer data previously bundled on node-red), converts it into the canonical dataformat as
specified in SensorThings and sends it in the Kafka message Bus.
If MQTT doesn't work, make sure that
1) you are listening on port 1883 (cmd: netstat -a)
2) mosquitto is running (cmd path/to/mosquitto mosquitto)
3) you can listen to the incoming MQTT data on chrome's MQTT Lens."""

import time
import re
import sys
import os
from datetime import datetime
import json
import logging

import pytz
# confluent_kafka is based on librdkafka, details in requirements.txt
from confluent_kafka import Producer, KafkaError
import paho.mqtt.client as mqtt
from logstash import TCPLogstashHandler

__author__ = "Salzburg Research"
__version__ = "1.2"
__date__ = "4 Dezember 2017"
__email__ = "christoph.schranz@salzburgresearch.at"
__status__ = "Development"

MQTT_BROKER = "il050.salzburgresearch.at"

LOGSTASH_HOST = os.getenv('LOGSTASH_HOST', 'localhost')
LOGSTASH_PORT = int(os.getenv('LOGSTASH_PORT', '5000'))

# kafka parameters
# topics and servers should be of the form: "topic1,topic2,..."
KAFKA_TOPIC = "SensorData"
BOOTSTRAP_SERVERS_default = 'il061,il062,il063'
KAFKA_GROUP_ID = "db-adapter"

# The mapping between incoming and outgoing metrics is defined by
# the json file located on:
dir_path = os.path.dirname(os.path.realpath(__file__))
datastream_map = os.path.join(dir_path, "datastreams.json")
blacklist_map = os.path.join(dir_path, "blacklist.json")

with open(datastream_map) as ds_file:
    DATASTREAM_MAPPING = json.load(ds_file)
with open(blacklist_map) as bl_file:
    BLACKLIST = json.load(bl_file)

# Define Kafka Producer
conf = {'bootstrap.servers': BOOTSTRAP_SERVERS_default}
producer = Producer(**conf)

# setup logging
logger = logging.getLogger('iot-adapter')
logger.setLevel(os.getenv('LOG_LEVEL', logging.INFO))
console_logger = logging.StreamHandler(stream=sys.stdout)
console_logger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logstash_handler = TCPLogstashHandler(host=LOGSTASH_HOST, port=LOGSTASH_PORT, version=1)
[logger.addHandler(l) for l in [console_logger, logstash_handler]]
logger.info('Sending logstash to %s:%d', logstash_handler.host, logstash_handler.port)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


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
    logger.info("Connection to {} on port {} established".format(MQTT_BROKER, 1883))
    client.loop_forever()


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
    datapoints = mqtt_to_sensorthings(msg)

    for datapoint in list(datapoints):
        if datapoint is None:
            return None

        datapoint["ts"] = convert_timestamp(datapoint["ts"])

        datapoint = convert_datastream_id(datapoint)
        if datapoint:
            message = create_message(datapoint)

            publish_message(message)
            logger.debug(
                "\tSent:\t%-25s %-40s%-10s" % (datapoint["quantity"], datapoint["ts"], datapoint["value"]))
            time.sleep(0)


def on_connect(client, userdata, flags, rc):
    """Report if connection to MQTT_BROKER is established
    and subscribe to all topics. MQTT subroutine"""
    logger.info("Connected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")


def on_disconnect(client, userdata, rc):
    """Reporting if connection to MQTT_BROKER is lost. MQTT subroutine"""
    logger.warning("Disconnect, reason: " + str(rc))
    logger.warning("Disconnect, reason: " + str(client))


def mqtt_to_sensorthings(msg):
    """
    Parsing MQTT message from raw message object and transform into inte
    :param msg: MQTT message including topic and payload
    :return: datapoint dictionary including quantity (topic), timestamp and value
    """
    datapoint = dict()
    datapoint["quantity"] = msg.topic

    payload = json.loads(msg.payload.decode("utf-8"))
    if type(payload) in [type(0), type(0.0)]:
        datapoint["value"] = payload
        datapoint["ts"] = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
        datapoints = list([datapoint])
    elif len(payload.keys()) != 2:
        # In this case there are multiple keys in one payload,
        # e.g. {'posE': 2233.81, 'posZ': 0.5, 'ts': 1516356558.740686, 'posX': 94.44, 'posY': 85.1}
        datapoints = fetch_multiple_data(payload)
        # return None
    else:
        try:
            datapoint["ts"] = payload["ts"]
            for key in list(payload.keys()):
                if key != "ts":
                    datapoint["value"] = payload[key]
            datapoints = list([datapoint])
        except:
            logger.error("Could not parse: {}".format(payload))
            return None
    return datapoints


def fetch_multiple_data(payload):
    """
    Previously used for parsing kafka messages
    :param msg: old kafka message
    :return: datapoint dictionary
    """
    datapoints = list()
    data_of_interest = ["posX", "posY", "posZ", "posE", "targetNozzle", "targetBed", "tempNozzle", "tempBed"]

    ts_in = payload.get("ts", None)
    if ts_in is None:  # return if no timestamp is found in payload
        return None

    for key, value in payload.items():
        if key in data_of_interest:
            datapoint = dict()
            datapoint["value"] = value
            datapoint["quantity"] = "rasp02/{}".format(key)
            datapoint["ts"] = ts_in
            datapoints.append(datapoint)
    return datapoints



def convert_datastream_id(datapoint):
    """
    Translate message metric: the MQTT input topic into the kafka bus quantity name via the
    metrics mapping json file.
    :param datapoint: dictionary including quantity, timestamp and value
    :return: updated datapoint dictionary including timestamp, value and new quantity
    :rtype: dictionary. None if datapoint is ignored.
    """
    try:
        datapoint["quantity"] = DATASTREAM_MAPPING[datapoint["quantity"]]
        return datapoint
    except KeyError:
        if datapoint["quantity"] not in BLACKLIST.keys():
            logger.warning("Ignoring {}".format(datapoint))
        pass
    return None


def create_message(datapoint):
    """
    Convert any datapoint message into the canonical message format
    from i-maintenance's first iteration.
    :param datapoint: dictionary including quantity, timestamp and value
    :return: canonical message format as 4-key dictionary
    """
    message = {
        'phenomenonTime': datapoint["ts"],
        'resultTime': datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat(),
        'result': datapoint["value"],
        'Datastream': {'@iot.id': datapoint["quantity"]}}
    return message


def convert_timestamp(ts_in, form="ISO8601"):
    """Parse any timestamp format into a specific one.
    Keyword arguments:
    :param ts_in: timestamp (string, int or float)
    :param form: timestamp format (string) default: "ISO8601"
    :return: a conventional timestamp of default type ISO8601 e.g. "2017-10-17T11:52:42.123456"
    """
    # Check if the input has already the correct format
    if form == "ISO8601" and type(ts_in) is str and ts_in.count(":") == 3:
        return ts_in
    # extract the unix timestamp to seconds
    if type(ts_in) in [int, float]:
        ts_len = len(str(int(ts_in)))
        if ts_len in [10, 9]:
            ts_in = float(ts_in)
        elif ts_len in [13, 12]:
            ts_in = float(ts_in) / 1000
        elif ts_len in [16, 15]:
            ts_in = float(ts_in) / 1000 / 1000
        else:
            logger.warning("Unexpected timestamp format: {} ({})".format(ts_in, type(ts_in)))
    if form == "ISO8601":
        return datetime.fromtimestamp(float(ts_in), pytz.UTC).isoformat()
    elif form in ["UNIX", "Unix", "unix"]:
        return ts_in
    else:
        logger.warning("Invalid date format specified: {}".format(form))


# noinspection PyBroadException
def publish_message(message):
    """
    Publish the canonical data format (Version: i-maintenance first iteration)
    to the Kafka Bus.

    Keyword argument:
    :param message: dictionary with 4 keywords
    :return: None
    """
    try:
        producer.produce(KAFKA_TOPIC, json.dumps(message).encode('utf-8'))
    except:
        logger.exception("Exception while sending: {} \non kafka topic: {}".format(message, KAFKA_TOPIC))


if __name__ == '__main__':
    define_mqtt_statemachine()
