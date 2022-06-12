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


# value_deserializer=lambda x: loads(x.decode('utf-8')))


@app.route('/hola')
def hello_world():  # put application's code here
    collection.insert_one({"sentiment": "TEST MESSAGE"})
    return "TEST"


# async def run():


if __name__ == '__main__':
    # listen_kill_server()
    while True:
        print("....listening kafka")
        # this method should auto-commit offsets as you consume them.
        # If it doesn't, turn on logging.DEBUG to see why it gets turned off.
        # Not assigning a group_id can be one cause
        # logging.DEBUG
        print(consumer.topics())
        for message in consumer:
            print(".....ongoing transaction")
            # consumed_message = json.loads(message.value.decode())
            print(message)
            collection.insert_one({"sentiment": "NEW MESSAGE"})
            print(message.topic)
            print(message.partition)
            print(message.offset)
            print(message.key)
            print(consumer.topics())
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
