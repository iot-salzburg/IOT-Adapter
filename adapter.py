import csv
import time
import re
import sys
from datetime import datetime
import json
import threading
import logging
import pandas as pd
import pytz
from kafka import KafkaConsumer, KafkaProducer

KAFKA_TOPIC_IN = "node-red-message"
KAFKA_TOPIC_OUT = "PrinterData"
BOOTSTRAP_SERVERS = ['il061:9092', 'il062:9092', 'il063:9092']

consumer = KafkaConsumer('node-red-message',
        bootstrap_servers=['il061', 'il062', 'il063'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        api_version=(0, 9))
consumer.subscribe([KAFKA_TOPIC_IN])

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         api_version=(0, 9),
        value_serializer=lambda m: json.dumps(m).encode('ascii'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def forward():
    for msg in consumer:
        data = json.dumps(msg)
        var = str(msg).split('value={')[1].split('}')[0] + ","
        print(var.count(","), msg)

        datapoint = fetch_sensor_data(msg)
        if datapoint is None:
            continue

        message = convert_to_sensorthings(datapoint)
        publish_message(message)


def fetch_sensor_data(msg):
    try:
        data = json.dumps(msg)
        if "airquality" in data:
            string = str(msg).split("'data':")[1].split('}')[0] + ","

            value_in = string.split("'level':")[1].split(',')[0]
            value_in = float(re.sub('[^.0-9]+', '', value_in))

            ts_in = string.split("'ts':")[1]
            ts_in = float(re.sub('[^0-9]+', '', ts_in))

            topic_in = str(msg).split("topic':")[1].split(",")[0]
            topic_in = topic_in.replace("'", "")

        elif "rasp06/temp" in data:
            var = str(msg).split('value={')[1].split('}')[0] + ","

            value_in = var.split("'data':")[1].split("'temp':")[1].split(",")[0]
            value_in = float(re.sub('[^.0-9]+', '', value_in))

            ts_in = var.split("'ts':")[1].split(",")[0]
            ts_in = float(re.sub('[^0-9]+', '', ts_in)[:13])

            topic_in = str(msg).split("topic':")[1].split("}")[0].split(",")[0]
            topic_in = topic_in.replace("'", "")

        elif "rasp02/um2-pos" in data:
            var = str(msg).split('value={')[1].split('}')[0] + ","
            if "posX" in var and "posZ" in var:
                return None
            for pos in ["posX", "posY", "posZ", "posE"]:
                if pos in var:
                    value_in = var.split("'data':")[1].split(pos)[1].split(",")[0]
                    value_in = float(re.sub('[^.0-9]+', '', value_in))
                    topic_in = "rasp02/um2-" + pos

            ts_in = var.split("'ts':")[1].split(",")[0]
            ts_in = float(re.sub('[^0-9]+', '', ts_in)[:13])

        elif "rasp02/um2-temp" in data:
            var = str(msg).split('value={')[1].split('}')[0] + ","
            if "targetNozzle" in var and "tempBed" in var:
                return None
            for temp in ["targetNozzle", "targetBed", "tempNozzle", "tempBed"]:
                if temp in var:
                    value_in = var.split("'data':")[1].split(temp)[1].split(",")[0]
                    value_in = float(re.sub('[^.0-9]+', '', value_in))
                    topic_in = "rasp02/um2-" + temp

            ts_in = var.split("'ts':")[1].split(",")[0]
            ts_in = float(re.sub('[^0-9]+', '', ts_in)[:13])


        elif "filaConsumption" in data:
            var = str(msg).split('value={')[1].split('}')[0] + ","

            value_in = var.split("'data':")[1].split("'filaConsumption':")[1].split(",")[0]
            value_in = float(re.sub('[^.0-9]+', '', value_in))

            ts_in = var.split("'ts':")[1].split(",")[0]
            ts_in = float(re.sub('[^0-9]+', '', ts_in)[:13])

            topic_in = str(msg).split("topic':")[1].split("}")[0].split(",")[0]
            topic_in = topic_in.replace("'", "")

        else:
            var = str(msg).split('value={')[1].split('}')[0] + ","

            value_in = var.split("data':")[1].split(",")[0]
            value_in = float(re.sub('[^.0-9]+', '', value_in))

            ts_in = var.split("ts':")[1].split(",")[0]
            ts_in = float(re.sub('[^.0-9]+', '', ts_in))

            topic_in = str(msg).split('value={')[1].split("topic':")[1].split(",")[0]
            topic_in = topic_in.replace("'", "")

        time.sleep(0.5)
        # print("ts: {}, \tdata: {},      \ttopic: {}".format(ts_in, value_in, topic_in))

    except:
        print("ExceptionRaised:\n")
        print(msg)
        sys.exit()

    datapoint = dict()
    datapoint["quantity"] = topic_in
    datapoint["ts"] = ts_in
    datapoint["value"] = value_in
    return datapoint


def convert_to_sensorthings(datapoint):
    phenomenon_time = transform_timestamp(datapoint["ts"], form="ISO8601")

    message = {
         'phenomenonTime': phenomenon_time,
         'resultTime': datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat(),
         'result': datapoint["value"],
         'Datastream': {'@iot.id': datapoint["quantity"]}}

    return message

def transform_timestamp(ts_in, form="ISO8601"):
    if type(ts_in) in [type(0), type(0.0)]:
        ts_len = len(str(int(ts_in)))
        if ts_len in {10,9}:
            observation_time = float(ts_in)
        elif ts_len in {13,12}:
            observation_time = ts_in/1000.0
        elif ts_len in {16,15}:
            observation_time = ts_in/1000.0/1000.0
        else:
            print("Unexpected timestamp format: {}".format(ts_in))
    if form == "ISO8601":
        return datetime.fromtimestamp(float(observation_time), pytz.UTC).isoformat()
    elif form in ["UNIX", "Unix", "unix"]:
        return observation_time


def publish_message(message):
    producer.send(KAFKA_TOPIC_OUT, message)
    pass


if __name__ == '__main__':
    forward()
