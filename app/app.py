from flask import Flask
import logging

from kafka import KafkaConsumer
from transformers import pipeline
from pymongo import MongoClient
import json
import threading

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
data = ["I love you. I hate you."]
sentiment_pipeline(data)
istrue = True
# KAFKA
from kafka import KafkaConsumer, TopicPartition

# consumer.subscribe(topics=['sentimentPython'])
# consumer.subscription()

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


@app.route('/hola')
def hello_world():  # put application's code here
    collection.insert_one({"sentiment": "TEST MESSAGE"})
    return "TEST"


if __name__ == '__main__':
    # listen_kill_server()
    while True:
        for message in consumer:
            print(".....ongoing transaction")
            collection.insert_one({"sentiment": message.value.decode()})
            # json.loads(message.value.decode())
    app.run(host="0.0.0.0", port=8083, debug=True)
    # while istrue:
    #     run()
    # consumer = Consumer()
    # To consume from fintechexplained-topic
    # print(consumer.topics())
    # for message in consumer:
    #     collection.insert_one({"sentiment": "TEST MESSAGE"})
    #     print(message.topic)
    #     print(message.partition)
    #     print(message.offset)
    #     print(message.key)
    #     print(consumer.topics())

    #     # consumer.poll()
    #     # consumer.seek_to_end()
    #     for message in consumer:
    #         message = message.value
    #         print(message.value)
    #         collection.insert_one({"message": message})
    #         print('{} added to {}'.format(message, collection))
