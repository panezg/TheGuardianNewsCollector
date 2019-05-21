from collections import deque
from datetime import date, timedelta
import requests
import os
import json
import logging
from time import sleep

directory_root = '/Users/gpanez/Documents/news/the_guardian'

class RequestError(Exception):
    pass


class Article:

    def __init__(self, article_json, current):
        self.priv_article_json = article_json
        self.current = current
        self.hs = {}
        self.hs['webPublicationDate'] = article_json['webPublicationDate']
        self.hs['id'] = article_json['id']
        self.hs['webUrl'] = article_json['webUrl']
        self.hs['webTitle'] = article_json['webTitle']
        self.hs['sectionId'] = article_json['sectionId']
        self.hs['sectionName'] = article_json['sectionName']
        self.hs['body'] = article_json['fields']['body']

    def save(self):
        directory = directory_root + '/' + self.current
        if not os.path.exists(directory):# or not os.path.isdir(directory):
            logging.debug('Creating directory: [%s]', directory_root)
            os.makedirs(directory)

        try:
            with open(directory + '/' + self.hs['id'].replace('/', '_') + '.txt', "w") as file:
                json.dump(self.hs, file)
        except IOError:
            logging.error('Couldn\'t create the file: [%s]', directory + '/' + self.hs['id'].replace('/', '_') + '.txt')

    def __str__(self):
        return json.dumps(self.hs)
        # return ''.join(['webPublicationDate:' + self.webPublicationDate,
        #                'id: ' + self.id,
        #                'webUrl: ' + self.webUrl,
        #                'webTitle: ' + self.webTitle,
        #                'sectionId: ' + self.sectionId,
        #                'sectionName: ' + self.sectionName,
        #                'body: ' + self.body])


class WorkItem:
    tries_threshold = 3

    def __init__(self, date_to_process):
        self.current = date_to_process
        self.str_current = date_to_process.isoformat()
        self.tries = 0

    #return tuple (continue_flag, list_articles)
    #continue_flag True indicates more pages need to be fetched
    def process_request_page(self, page_num = 1):
        url = "https://content.guardianapis.com/search"
        payload = {'api-key': '6d2daecd-e66c-403d-b3c2-567cb7e0e2cc',
                   'section': '-commentisfree',
                   'tag': '(us-news/us-politics) | (politics/politics)',
                   'show-fields': 'body',
                   'page-size': '50',
                   'order-by': 'oldest',
                   'page': page_num,
                   'lang': 'en',
                   'from-date': self.str_current,
                   'to-date': self.str_current}
        resp = requests.get(url, params = payload)
        logging.info('Sending HTTP request for work item with date: [%s]', self.str_current)

        if resp.status_code == 200:
            parsed_json = resp.json()
            if parsed_json['response']['status'] == 'ok':
                articles_json = parsed_json['response']['results']
                articles = []
                for article_json in articles_json:
                    if article_json['type'] != 'article':
                        logging.debug(
                            'Skipping hit with id: [%s] since it is of undesired type: [%s] from work item with date: [%s]',
                            article_json['id'], article_json['type'], self.str_current)
                    else:
                        articles.append(Article(article_json, self.str_current))
                        #print(hex(id(art)))
                if page_num < parsed_json['response']['pages']:
                    return True, articles
                else:
                    return False, articles
            else:
                logging.warning('API response failed. Response status: [%s] for work item with date: [%s]. Entire json: [%s]',
                                parsed_json['response']['status'], self.str_current, parsed_json)
                self.tries += 1
                logging.debug('Incrementing retry count to: [%s] for work item with date: [%s]', self.tries,
                              self.str_current)
                raise RequestError
        else:
            logging.warning('HTTP response failed. Status code: [%s] for work item with date: [%s]. Entire response: [%s]', resp.status_code,
                            self.str_current, resp)
            self.tries += 1
            logging.debug('Incrementing retry count to: [%s] for work item with date: [%s]', self.tries, self.str_current)
            raise RequestError


    def process(self):
        try:
            full_articles = []
            continue_flag, articles = self.process_request_page()
            i = 1
            full_articles += articles
            while continue_flag:
                i += 1
                # Up to 12 calls per second
                # Up to 5,000 calls per day
                sleep(0.1)
                continue_flag, articles = self.process_request_page(page_num = i)
                full_articles += articles
            for article in full_articles:
                article.save()
                logging.info('Saving to disk hit with id: [%s] from work item with date: [%s]', article.hs['id'], self.str_current)
            return True
        except RequestError:
            if self.tries < self.tries_threshold:
                return False
            else:
                logging.error('Reached maximum number of tries for work item with date: [%s]', self.str_current)
                return True


def generate_work_items(start, end):
    work_items_queue = deque([])
    date_to_process = start
    delta = timedelta(days=1)

    while date_to_process <= end:
        work_items_queue.append(WorkItem(date_to_process))
        date_to_process = date_to_process + delta
    return work_items_queue


def process_items():
    start = date(2018, 1, 1)
    end = date(2018, 12, 31)
    #end = date(2019, 12, 31)
    #2003
    queue = generate_work_items(start, end)

    while len(queue) != 0:
        work_item = queue.popleft()
        if not work_item.process():
            queue.append(work_item)


def main():
    logging.basicConfig(filename=directory_root + '/log.txt',
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)
    logging.info("The Guardian News Collector")
    logging.getLogger('TGNC')
    process_items()
    # TODO: Added timing thresholds but need to add limit per day
    # TODO: Added timing thresholds but need to add saving the queue
    # TODO: Need to add cron job, and overall begin and end


if __name__ == "__main__":
    main()
    print("done")