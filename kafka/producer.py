import json
import datetime
import numpy as np
import sys
import time
from kafka import KafkaProducer
from.config import kafka_config

producer = KafkaProducer(bootstrap_servers=kafka_config.bootstrap_server,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def get_time():
    today = datetime.datetime.now()
    time_s = today.strftime("%Y-%m-%d %H:%M:%S")
    return time_s


def parse_links():
    # todo parse link
    link_data = {"link": "ЛИНК", "time": get_time()}
    return link_data


def parse_comments():
    # todo parse comments
    comment_data = {"comment": "КОММЕНТ", "time": get_time()}
    return comment_data


while True:
    link_data = parse_links()
    comment_data = parse_comments()

    producer.send(kafka_config.link_topic, value=link_data)
    producer.send(kafka_config.comment_topic, value=comment_data)
    print(link_data)
    print(comment_data)
    time.sleep(1)