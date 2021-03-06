import codecs
import csv

import requests
from bs4 import BeautifulSoup

from collector.comment_data import COMMENT_DATA_X_Y
from collector.extraction import get_comment_data_list_by_link
from common.utils import get_config, get_bot_ids, get_vk_api, PROCESSED_NOTIFY_NUM

BOTS_NUM = 'bots_num'
BOT_DATA_PATH = '../../data/bots.tsv'
GET_BOT_URL_FORM = 'https://gosvon.net/?usr={}'
GROUP_LINK_NAME = 'Ссылка'
BANNED = 'Страница забанена'


def main():
    vk_api = get_vk_api()
    bot_ids = get_bot_ids()
    with codecs.open(BOT_DATA_PATH, 'w+', encoding='utf8') as bot_data_file:
        tsv_writer = csv.writer(bot_data_file, delimiter='\t')
        tsv_writer.writerow(COMMENT_DATA_X_Y)

        limit = get_config()[BOTS_NUM]
        if limit == 0:
            return
        cnt = 0
        # start_time = time.time()

        for bot_id in bot_ids:
            response = requests.get(GET_BOT_URL_FORM.format(bot_id))
            if BANNED in response.text:
                continue

            soup = BeautifulSoup(response.text, features='html.parser')
            for a in soup.findAll('a'):
                content = a.contents
                if GROUP_LINK_NAME in content:
                    link = a['href']
                    data_list = get_comment_data_list_by_link(vk_api, link, True)
                    if data_list is None:
                        continue

                    tsv_writer.writerow(data_list)
                    cnt += 1

                    if cnt % PROCESSED_NOTIFY_NUM == 0:
                        print('Processed {} bots...'.format(cnt))
                        bot_data_file.flush()

                    if cnt == limit:
                        # print("--- %s seconds ---" % (time.time() - start_time))
                        return


if __name__ == '__main__':
    main()
