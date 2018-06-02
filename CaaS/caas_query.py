# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# 6/1/18
# updated 6/1/18

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


class CaaSRecord:
    __slots__ = ('title', 'url', 'tags', 'caas_id', 'cms_id', 'gnlp_categories', 'wnlp_categories')

    def __init__(self, title='', url='', caas_id='', cms_id='', tags=[], gnlp_categories=None, wnlp_categories=None):
        self.title = title
        self.url = url
        self.caas_id = caas_id
        self.cms_id = cms_id
        self.tags = tags
        self.gnlp_categories = gnlp_categories
        self.wnlp_categories = wnlp_categories

    def __repr__(self):
        return f'Row({self.title}, {self.url}, {self.tags}, {self.caas_id}, {self.cms_id}, {self.gnlp_categories}, {self.wnlp_categories})'


def _initialize_logger(name):
    logger = logging.getLogger(name)
    logger.info('{} logger instantiated'.format(name))
    return logger


def configure_logger():
    with open('utils/log.yaml', 'r') as log_conf:
        log_config = yaml.safe_load(log_conf)

    log_config['handlers']['file']['filename'] = log_file
    logging.config.dictConfig(log_config)
    logging.info('* * * * * * * * * * * * * * * * * * * *')
    logging.info('logging configured')

    return _initialize_logger('query')


def _check_file(output):
    '''confirm overwrite of an existing file'''
    if os.path.isfile(output):
        print('the specified output file already exists, do you want to overwrite it?')
        overwrite = input('y/n: ').lower()
        return output if overwrite.lower().startswith('y') else None
    else:
        return output


def capture_args():
    '''
    capture any arguments supplied to the script on the command line. if an output
    file is specified, check if it already exists and confirm overwrite if it does
    '''
    if len(sys.argv) > 2:
        print('too many arguments')
        print('usage: python3 query.py output_file.csv')
        raise SystemExit('output_file.csv is optional')
    elif len(sys.argv) == 2:
        output = sys.argv[1]
        output = os.path.join('output', output)
        return _check_file(output)


def extract_caas_data(entry):
    # non-pronto data
    caas_id = rsp['$']['id']
    cms_id = rsp['cms_id']
    taxonomy = rsp['si']['taxonomy']['tags']
    type = rsp['$type'][0]['$id'].split('/')[1]
    url = rsp['web_article_url']
    url_alt = rsp['asset_url']
    title = rsp['$name']
    gnlp_id = rsp["$i_nlp_source_google"][0]['$id']
    wnlp_id = rsp["$i_nlp_source_watson"]


def extract_pronto_data(entry):
    keys = {
        'title': None,
        'url': None,
        'tag': None
    }

    # pronto data
    pronto_data = entry['pronto']['article']


if __name__ == '__main__':
    logger = configure_logger()
    caas_client = client_wrapper.CaaSClient(elastic_path=elastic_path, query_config_path=query_config_path, logger=_initialize_logger('caas_client'))
    output_file = capture_args()

    response = caas_client.search()
    rsp = response[0]
    print(rsp['pronto'])
