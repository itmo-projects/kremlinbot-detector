from kafka.consumer import KafkaConsumer
from pymongo import MongoClient
from clickhouse_driver import Client, connect
import json


# from somewhere import kafka_config

def collect_comments():
    for message in consumer:
        message = message.value
        comment = json.dumps(message)
        client.execute("INSERT INTO test format JSONEachRow {}".format(comment))

def create_client():
    client = Client('localhost')
    client.execute('use trail')
    client.execute('truncate table test')
    client.execute("create table IF NOT EXISTS test(Business Float32,Political Float32, covid Float32,crime Float32, time DateTime('Europe/Moscow'), \
        sports Float32 ) ENGINE = MergeTree() ORDER BY time")


if __name__ == "__main__":
    consumer = KafkaConsumer(
        kafka_config.net_output_topic,
        bootstrap_servers=[kafka_config.bootstrap_server],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    client = create_client()

    collect_comments()
