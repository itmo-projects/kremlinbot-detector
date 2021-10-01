import pickle
import datetime
import json
import pandas as pd

from kafka import KafkaConsumer, KafkaProducer
from kafka.config import kafka_config

from analyzer.predict import get_answer
from collector.comment_data import COMMENT_DATA_X

MODEL_PATH = '../resources/mystem_nltk-stopwords_badwords_RandomForestClassifier_model.pkl'


def main():
    consumer = KafkaConsumer(
        kafka_config.comment_topic,
        bootstrap_servers=kafka_config.bootstrap_server,
        value_deserializer=bytes.decode
    )

    producer = KafkaProducer(
        bootstrap_servers=kafka_config.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    with open(MODEL_PATH, 'rb') as file:
        model, mapper, min_max_scaler = pickle.load(file)

    while True:
        message_batch = consumer.poll()

        for topic_partition, partition_batch in message_batch.items():
            for message in partition_batch:
                x = message.value
                x_df = pd.DataFrame([x], columns=COMMENT_DATA_X)

                predict = get_answer(model, mapper, min_max_scaler, x_df)
                prediction_dict = {'predict': predict, "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

                producer.send(kafka_config.net_output_topic, value=prediction_dict)
                consumer.commit()


if __name__ == '__main__':
    main()
