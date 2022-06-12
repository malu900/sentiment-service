import logging

from flask import Flask
from pymongo import MongoClient
from transformers import pipeline

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
# data = ["I love you. I hate you."]
# sentiment_pipeline(data)
istrue = True
# KAFKA
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'sentimentPython',
    bootstrap_servers=['kafka.kafka.svc.cluster.local:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    fetch_max_wait_ms=0,
    group_id="My_python_group",
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
            pred = predictSentiment(message.value.decode())
            predict = pred[0]["label"]
            collection.insert_one({"sentiment": predict})

    app.run(host="0.0.0.0", port=8083, debug=True)
