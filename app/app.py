import logging

from flask import Flask
from pymongo import MongoClient
from transformers import pipeline
import json
from kafka import KafkaConsumer, KafkaProducer, admin
# APP
# configmap and environment vars
# https://www.magalix.com/blog/the-configmap-pattern
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
## MONGODB
cluster = MongoClient(
    "mongodb+srv://kwtr-mongodb:cPc9sKhVfv5VpRE@kwtr-cluster.f42nfyr.mongodb.net/?retryWrites=true&w=majority")
db = cluster["sentiment"]
collection = db["sentiment"]
# PIPELINE
sentiment_pipeline = pipeline("sentiment-analysis")

consumer = KafkaConsumer(
    'sentimentObjectPython',
    bootstrap_servers=['kafka.kafka.svc.cluster.local:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    fetch_max_wait_ms=0,
    group_id="My_python_group2",
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    max_poll_interval_ms=5000,
    max_poll_records=1
)


def predictSentiment(msg):
    return sentiment_pipeline(msg)


@app.route('/hola')
def hello_world():  # put application's code here
    collection.insert_one({"sentiment": "TEST MESSAGE"})

    return "TEST"


if __name__ == '__main__':
    while True:
        for message in consumer:
            # msg = message.value.decode()
            msg = message.value

            # predict = pred[0]["label"]
            values_view = msg.values()
            value_iterator = iter(values_view)
            first_value = next(value_iterator)

            # collection.insert_one({"sentiment": msg})
            pred = predictSentiment(first_value)
            predict = pred[0]["label"]
            test = collection.insert_one({"sentiment": predict})
            idd = str(test.inserted_id)
            collection.insert_one({"sentiment": idd})

            producer = KafkaProducer(bootstrap_servers='kafka.kafka.svc.cluster.local:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            producer.send('sentimentTopicSpringReturnTwo', {"id": msg["id"], "sentiment": predict, "sentimentId": idd})

    app.run(host="0.0.0.0", port=8083, debug=True)
