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


class QueryData:

    def __init__(self, query_response):
        self.response = query_response
        self.records = {i: self._init_record() for i in range(len(self.response))}
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
                'gnlp_categories': [],
                'wnlp_categories': []
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
            self.records[caas_id]['wnlp_id'] = entry["$i_nlp_source_watson"] if "$i_nlp_source_watson" in entry.keys() else None


if __name__ == '__main__':
    logger = configure_logger()
    output_file = capture_args()
    caas_client = client_wrapper.CaaSClient(elastic_path=elastic_path,
                                            query_config_path=query_config_path,
                                            logger=_initialize_logger('caas_client'))

    response = caas_client.search()
    rsp = response[0]
    print(rsp['pronto'])
