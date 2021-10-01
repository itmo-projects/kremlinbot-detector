import json
import re
import time

from kafka import KafkaConsumer
from kafka.producer.kafka import KafkaProducer

from collector.extraction import get_comment_data_list
from common.utils import get_vk_api
from kafka.config import kafka_config

GROUP_REGEX = r'^https://vk.com/(.+)'
MAX_COUNT = 100
MAX_OFFSET = 1000


def main():
    consumer = KafkaConsumer(
        kafka_config.link_topic,
        bootstrap_servers=kafka_config.bootstrap_server,
        value_deserializer=bytes.decode
    )

    producer = KafkaProducer(
        bootstrap_servers=kafka_config.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    vk_api = get_vk_api()

    for message in consumer:
        group_link = message.value
        match = re.match(GROUP_REGEX, group_link)
        group_domain = match.group(1)

        try:
            group_object = vk_api.utils.resolveScreenName(screen_name=group_domain)
        except:
            continue
        if len(group_object) == 0:
            continue

        group_id = group_object['object_id']
        for post_offset in range(0, MAX_OFFSET, MAX_COUNT):
            try:
                post_items = \
                    vk_api.wall.get(owner_id=-group_id, offset=post_offset, count=MAX_COUNT, filter='owner')[
                        'items']
            except:
                continue
            post_ids = map(lambda item: item['id'], post_items)

            for post_id in post_ids:
                for comment_offset in range(0, MAX_OFFSET, MAX_COUNT):
                    try:
                        comment_items = \
                            vk_api.wall.getComments(owner_id=-group_id, post_id=post_id, offset=comment_offset,
                                                    count=MAX_COUNT)['items']
                    except:
                        continue
                    politician_item_ids = filter(
                        lambda item: item.get('from_id') is not None and item.get('from_id') not in bot_ids,
                        comment_items)
                    comment_ids = map(lambda item: item['id'], politician_item_ids)

                    for comment_id in comment_ids:
                        data_list = get_comment_data_list(vk_api, -group_id, comment_id, False)
                        if data_list is None:
                            continue

                        producer.send(kafka_config.net_output_topic, value=data_list)

        time.sleep(2)


if __name__ == '__main__':
    main()
