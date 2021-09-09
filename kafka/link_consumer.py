from kafka import KafkaConsumer
from .config import kafka_config
import json

consumer_links = KafkaConsumer(
     kafka_config.link_topic,
     bootstrap_servers=[kafka_config.bootstrap_server],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))


for message in consumer_links:
    message = message.value
    some_other_dict = json.dumps(message)
    # todo do with it smth
    print(some_other_dict)