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


def forward_message():
    for msg in consumer:

        datapoint = fetch_sensor_data(msg)
        if datapoint is None:
            continue

        print("Datapoint: {}".format(datapoint))

        message = convert_to_sensorthings(datapoint)
        publish_message(message)

        time.sleep(0.4)


def fetch_sensor_data(msg):
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
                    datapoint["quantity"] = "rasp02/um2-" + pos
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
                    datapoint["quantity"] = "rasp02/um2-" + temp
            ts_in = var.split("'ts':")[1].split(",")[0]
            datapoint["ts"] = float(re.sub('[^0-9]+', '', ts_in)[:13])

        if structure == 1:
            var = str(msg).split('{')[1].split('}')[0] + ","
            value_in = var.split("data")[1].split(",")[0]
            datapoint["value"] = float(re.sub('[^.0-9]+', '', value_in))
            ts_in = var.split("ts")[1].split(",")[0]
            datapoint["ts"] = float(re.sub('[^.0-9]+', '', ts_in))

            topic_in = var.split("topic")[1].split(":")[1].split(",")[0]
            datapoint["quantity"] = topic_in.replace("'", "").replace('"','').strip()

        elif structure == 2:
            inner = str(msg).split('{')[2].split("}")[0].split(",")
            for metric in inner:
                if "'ts':" in metric:
                    datapoint["ts"] = float(re.sub('[^.0-9]+', '', metric))
                else:
                    datapoint["value"] = float(re.sub('[^.0-9]+', '', metric))

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
    forward_message()
