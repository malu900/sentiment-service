from kafka import KafkaConsumer
import logging
from pymongo import MongoClient
cluster = MongoClient("mongodb+srv://kwtr-mongodb:cPc9sKhVfv5VpRE@kwtr-cluster.f42nfyr.mongodb.net/?retryWrites=true&w=majority")
db = cluster["sentiment"]
collection = db["sentiment"]

logging.basicConfig(level=logging.INFO)


class Consumer:

    def __init__(self):
        self._init_kafka_consumer()

    def _init_kafka_consumer(self):
        self.kafka_host = "kafka.kafka.svc.cluster.local:9092"
        self.kafka_topic = "sentimentPython"
        self.consumer = KafkaConsumer(
            "sentimentPython",
            bootstrap_servers=self.kafka_host,
        )

    def consume_from_kafka(self):
        for message in self.consumer:
            collection.insert_one({"sentiment": message.value})
            print("%s:%d:%d: value=%s" % (message.topic, message.partition, message.value))
            logging.info(message.value)

