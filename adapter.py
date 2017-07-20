#!/usr/bin/env python3
#  -*- coding: utf-8 -*-


"""adapter.py: This module fetches heterogenious data from the Iot-Lab's MQTT_BORKER
(3D printer data previously bundled on node-red), converts it into the canonical dataformat as
specified in SensorThings and sends it in the Kafka message Bus."""

__author__ = "Salzburg Research"
__version__ = "1.1"
__email__ = "christoph.schranz@salzburgresearch.at"
__status__ = "Development"

import time
import re
import sys
from datetime import datetime
import json
import threading
import logging
import pytz
from kafka import KafkaProducer
import paho.mqtt.client as mqtt

MQTT_BROKER = "il050.salzburgresearch.at"
KAFKA_TOPIC_OUT = "PrinterData"
BOOTSTRAP_SERVERS = ['il061:9092', 'il062:9092', 'il063:9092']
METRIC_MAPPING = "metrics.json"

with open(METRIC_MAPPING) as metric_file:
    metric_dict = json.load(metric_file)

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         api_version=(0, 9),
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

keylist = list()


def forward_message():
    """Setting up connection to MQTT_BROKER and wait for messages    """
    # The protocol must be specified in python!
    client = mqtt.Client(protocol=mqtt.MQTTv31)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.connect(MQTT_BROKER, 1883, 60)
    logger.info("Connection to {} on port {} established".format(MQTT_BROKER, 1883))
    client.loop_forever()


def on_message(client, userdata, msg):
    """Action on received message.
    Nothing to return, because message sending is called here."""
    datapoint = fetch_mqtt_msg(msg)
    if datapoint is None:
        return None
    logger.info("New message received: {} {}".format(msg.topic, str(msg.payload)))
    # print(msg.payload)
    datapoint["ts"] = translate_timestamp(datapoint["ts"])

    datapoint = transform_metric(datapoint)
    print("\t%-25s %-40s%-10s" % (datapoint["quantity"], datapoint["ts"], datapoint["value"]))
    if datapoint["quantity"] not in keylist:
        keylist.append(datapoint["quantity"])
        # print("\tDatapoint: {}".format(datapoint))

    message = convert_to_sensorthings(datapoint)

    publish_message(message)
    time.sleep(0)


def on_connect(client, userdata, flags, rc):
    """Report if connection to MQTT_BROKER is established
    and subscribe to topics."""
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")

def on_disconnect(client, userdata, rc):
    """Reporting if connection to MQTT_BROKER is lost."""
    print("Disconnect, reason: " + str(rc))
    print("Disconnect, reason: " + str(client))


def fetch_mqtt_msg(msg):
    """
    Fetching MQTT message from MQTT_BROKER.
    :param msg: MQTT message including topic and payload
    :return: datapoint dictionary including quantity (topic), timestamp and value
    """
    datapoint = dict()
    datapoint["quantity"] = msg.topic

    payload = json.loads(msg.payload.decode("utf-8"))
    if type(payload) in [type(0), type(0.0)]:
        datapoint["value"] = payload
        datapoint["ts"] = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
    elif len(payload.keys()) != 2:
            # This case happens, if several values are sent in one message (e.g. 4 temps)
            return None
    else:
        try:
            datapoint["ts"] = payload["ts"]
            for key in list(payload.keys()):
                if key != "ts":
                    datapoint["value"] = payload[key]
        except:
            logger.error("Could not parse: {}".format(payload))
            return None
    return datapoint


def fetch_sensor_data(msg):
    """
    Previously used for parsing kafka messages
    :param msg: old kafka message
    :return: datapoint dictionary
    """
    try:
        datapoint = dict()
        structure = str(msg).count("{")

        if "rasp02/um2-pos" in str(msg):
            var = str(msg).split('value={')[1].split('}')[0] + ","
            if "posX" in var and "posZ" in var:
                return None
            for pos in ["posX", "posY", "posZ", "posE"]:
                if pos in var:
                    value_in = var.split("'data':")[1].split(pos)[1].split(",")[0]
                    datapoint["value"] = float(re.sub('[^.0-9]+', '', value_in))
                    datapoint["quantity"] = "rasp02/um2-{}".format(pos)
            ts_in = var.split("'ts':")[1].split(",")[0]
            datapoint["ts"] = float(re.sub('[^0-9]+', '', ts_in)[:13])

        elif "rasp02/um2-temp" in str(msg):
            var = str(msg).split('value={')[1].split('}')[0] + ","
            if "targetNozzle" in var and "tempBed" in var:
                return None
            for temp in ["targetNozzle", "targetBed", "tempNozzle", "tempBed"]:
                if temp in var:
                    value_in = var.split("'data':")[1].split(temp)[1].split(",")[0]
                    datapoint["value"] = float(re.sub('[^.0-9]+', '', value_in))
                    datapoint["quantity"] = "rasp02/um2-{}".format(temp)
            ts_in = var.split("'ts':")[1].split(",")[0]
            datapoint["ts"] = float(re.sub('[^0-9]+', '', ts_in)[:13])

        elif structure == 1:
            var = str(msg).split('{')[1].split('}')[0] + ","
            value_in = re.sub('[^.0-9]+', '', var.split("data")[1].split(",")[0])
            try:
                datapoint["value"] = float(value_in)
            except ValueError:
                datapoint["value"] = value_in

            ts_in = var.split("ts")[1].split(",")[0]
            datapoint["ts"] = float(re.sub('[^.0-9]+', '', ts_in))

            topic_in = var.split("topic")[1].split(":")[1].split(",")[0]
            datapoint["quantity"] = topic_in.replace("'", "").replace('"', '').strip()

        elif structure == 2:
            inner = str(msg).split('{')[2].split("}")[0].split(",")
            for metric in inner:
                if "'ts':" in metric:
                    datapoint["ts"] = float(re.sub('[^.0-9]+', '', metric))
                else:
                    value_in = re.sub('[^.0-9]+', '', metric)
                    try:
                        datapoint["value"] = float(value_in)
                    except ValueError:
                        datapoint["value"] = value_in

            var = "".join("".join(str(msg).split('{')[1:]).split("}")[:-1])
            topic_in = var.split("topic")[1].split(":")[1].split(",")[0]
            datapoint["quantity"] = topic_in.replace("'", "").replace('"', '').strip()

        else:
            print("\nFetched undefined input:")
            var = str(msg).split('value={')[1].split('}')[0] + ","
            value_in = var.split("data':")[1].split(",")[0]
            datapoint["value"] = float(re.sub('[^.0-9]+', '', value_in))
            ts_in = var.split("ts':")[1].split(",")[0]
            datapoint["ts"] = float(re.sub('[^.0-9]+', '', ts_in))

            topic_in = str(msg).split('value={')[1].split("topic':")[1].split(",")[0]
            datapoint["quantity"] = topic_in.replace("'", "")

    except:
        print("FetchingExceptionRaised:\n")
        print(msg)
        sys.exit()

    return datapoint


def transform_metric(datapoint):
    """
    Translate the MQTT input topic into the kafka bus quantity name.
    :param datapoint: dictionary including quantity, timestamp and value
    :return: updated datapoint dictionary including timestamp, value and new quantity
    :rtype: dictionary
    """
    try:
        datapoint["quantity"] = [list(i.values())[0] for i in metric_dict["metrics"]
                                 if list(i.keys())[0] == datapoint["quantity"]][0]
    except KeyError:
        logger.warning("No key found for: {}".format(datapoint))
    return datapoint


def convert_to_sensorthings(datapoint):
    """
    Convert any datapoint message into the canonical message format
    from i-maintenance's first iteration.
    :param datapoint: dictionary including quantity, timestamp and value
    :return: canonical message format as 4-key dictionary
    """
    phenomenon_time = translate_timestamp(datapoint["ts"], form="ISO8601")
    message = {
         'phenomenonTime': phenomenon_time,
         'resultTime': datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat(),
         'result': datapoint["value"],
         'Datastream': {'@iot.id': datapoint["quantity"]}}
    return message


def translate_timestamp(ts_in, form="ISO8601"):
    """Parse any timestamp format into a specific one.
    Keyword arguments:
    :param ts_in: timestamp (string, int or float)
    :param form: timestamp format (string) default: "ISO8601"
    :return: a conventional timestamp
    """
    # Check if the input has already the correct format
    if form == "ISO8601" and type(ts_in) is str and ts_in.count(":") == 3:
        return ts_in
    # extract the unix timestamp to seconds
    if type(ts_in) in [int, float]:
        ts_len = len(str(int(ts_in)))
        if ts_len in [10, 9]:
            observation_time = float(ts_in)
        elif ts_len in [13, 12]:
            observation_time = ts_in/1000.0
        elif ts_len in [16, 15]:
            observation_time = ts_in/1000.0/1000.0
    else:
        logger.warning("Unexpected timestamp format: {} ({})".format(ts_in, type(ts_in)))
        observation_time = ts_in
    if form == "ISO8601":
        return datetime.fromtimestamp(float(observation_time), pytz.UTC).isoformat()
    elif form in ["UNIX", "Unix", "unix"]:
        return observation_time
    else:
        logger.warning("Invalid date format specified: {}".format(form))


def publish_message(message):
    """Publish the canonical data format (Version: i-maintenance first iteration)
    to the Kafka Bus.

    Keyword argument:
    :param message: dictionary with 4 keywords
    :return: None
    """
    try:
        producer.send(KAFKA_TOPIC_OUT, message)
    except:
        logger.warning("Exception while sending: {} \non kafka topic: {}".format(message, KAFKA_TOPIC_OUT))


if __name__ == '__main__':
    forward_message()
