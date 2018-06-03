# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# 6/1/18
# updated 6/2/18

'''
config/query_config.json holds all search parameters other than the elasticsearch
request. these params are: 'type', 'provider', 'follow', and 'fields'.
refer to the CaaS API documentation for an explanation of these:
http://docs-caas.timeincapp.com/#search-and-get-examples

the thing that will change most often is the elastic_search_request.
paste your request into the file 'config/elastic_search_request.json'
the content to paste can be found by searching search.timeinc.com, then
clicking on the "Advanced" tab and pasting the code from there verbatim.
'''

import os
import sys
import csv
import yaml
import logging
import logging.config
from collections import namedtuple
from utils import client_wrapper

elastic_path = 'config/elastic_search_request.json'
query_config_path = 'config/query_config.json'
log_file = 'query.log'


def _initialize_logger(name):
    logger = logging.getLogger(name)
    logger.info('{} logger instantiated'.format(name))
    return logger


def _check_file(output):
    '''confirm overwrite of an existing file'''
    if os.path.isfile(output):
        logger.info('the specified output file already exists, do you want to overwrite it?')
        overwrite = input('y/n: ').lower()
        return output if overwrite.lower().startswith('y') else None
    else:
        return output


def configure_logger():
    with open('utils/log.yaml', 'r') as log_conf:
        log_config = yaml.safe_load(log_conf)

    log_config['handlers']['file']['filename'] = log_file
    logging.config.dictConfig(log_config)
    logging.info('* * * * * * * * * * * * * * * * * * * *')
    logging.info('logging configured')

    return _initialize_logger('query')


def capture_args():
    '''
    capture any arguments supplied to the script on the command line. if an output
    file is specified, check if it already exists and confirm overwrite if it does
    '''
    if len(sys.argv) > 2:
        logger.error('too many arguments')
        logger.info('usage: python3 query.py output_file.csv')
        raise SystemExit('output_file.csv is optional')
    elif len(sys.argv) == 2:
        output = sys.argv[1]
        output = os.path.join('output', output)
        return _check_file(output)


def get_gnlp_data(client, records):
    '''
    construct a dict formatted as gnlp_id: gnlp(caas_id, {}), where the value of
    gnlp_id is a namedtuple called gnlp that has 2 fields: caas_id and categories.
    then query CaaS for google NLP data for the entries in the argument records
    that have associated google nlp ids.

    currently this function is capturing 'nlp_categories' and their corresponding
    'confidence' level. other available fields are 'nlp_entities' and 'nlp_docSentiment'.

    any google nlp results that do not contain the field 'nlp_categories' are dropped.
    '''
    logger.info('getting available google nlp data for the current response')
    gnlp = namedtuple('gnlp', ['caas_id', 'categories'])
    gnlps = {records[key]['gnlp_id']: gnlp(key, {}) for key in records.keys() if records[key]['gnlp_id']}
    gnlp_ids = [key for key in gnlps.keys()]
    gnlp_data = client.get_batch(ids=gnlp_ids)

    for i in range(len(gnlp_data)):
        gnlp_id = gnlp_data[i]['$']['id']
        if 'nlp_categories' in gnlp_data[i].keys():
            gnlps[gnlp_id].categories['name'] = gnlp_data[i]['nlp_categories'][0]['name']
            gnlps[gnlp_id].categories['confidence'] = gnlp_data[i]['nlp_categories'][0]['confidence']

    return {key: value for key, value in gnlps.items() if value.categories}


def update_query_with_gnlp_data(records, gnlp_data):
    for key in gnlp_data.keys():
        caas_id = gnlp_data[key].caas_id
        records[caas_id]['gnlp_categories'] = gnlp_data[key].categories

    return records


class QueryData:

    def __init__(self, query_response):
        self.response = query_response
        self.records = self._init_records()
        self._extract_caas_data()

    def _init_records(self):
        return {
            self.response[i]['$']['id']: {
                'caas_id': self.response[i]['$']['id'],
                'cms_id': '',
                'title': '',
                'url': '',
                'brand': '',
                'gnlp_id': '',
                'wnlp_id': '',
                'gnlp_categories': {},
                'wnlp_categories': {}
            } for i in range(len(self.response))
        }

    def _extract_caas_data(self):
        '''
        extract data from a caas record

        some currently unextracted parameters:
        taxonomy = entry['si']['taxonomy']['tags']  # si (sports illustrated) specific
        type = entry['$type'][0]['$id'].split('/')[1]
        url_alt = entry['asset_url']
        pronto_data = entry['pronto']['article']
        content = entry['web_article_content']
        '''

        for i in range(len(self.response)):
            entry = self.response[i]
            caas_id = entry['$']['id']
            self.records[caas_id]['cms_id'] = entry['cms_id'] if 'cms_id' in entry.keys() else None
            self.records[caas_id]['title'] = entry['web_article_title'] if 'web_article_title' in entry.keys() else entry['$name']
            self.records[caas_id]['url'] = entry['web_article_url'] if 'web_article_url' in entry.keys() else None
            self.records[caas_id]['brand'] = entry['brand'] if 'brand' in entry.keys() else None
            self.records[caas_id]['gnlp_id'] = entry["$i_nlp_source_google"][0]['$id'] if "$i_nlp_source_google" in entry.keys() else None
            self.records[caas_id]['wnlp_id'] = entry["$i_nlp_source_watson"][0]['$id'] if "$i_nlp_source_watson" in entry.keys() else None


if __name__ == '__main__':
    logger = configure_logger()
    output_file = capture_args()
    caas_client = client_wrapper.CaaSClient(elastic_path=elastic_path,
                                            query_config_path=query_config_path,
                                            logger=_initialize_logger('caas_client'))

    response = caas_client.search()
    data = QueryData(response)
