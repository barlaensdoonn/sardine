# wrapper for legacy Time Inc's python 3 CaaS client hosted here:
# https://github.com/TimeInc/caas-content-client-python-3
# 5/21/18
# updated 6/3/18

import sys
import yaml
import json
import logging
import logging.config
from time import sleep

try:
    from . import caas_keys
except ImportError:
    import caas_keys


# add the caas python 3 client to our path so the script can use it
sys.path.insert(0, caas_keys.path_to_caas_module_home)
from caas_content_client_python_3 import client


example_elastic_search_request = {
    "size": 25,
    "from": 0,
    "query": {"match": {"_all": "hair"}},
    "sort": [{"$date": {"unmapped_type": "long", "order": "desc"}}]
}


class CaaSClient:
    '''
    wrapper for legacy Time Inc's python3 CaaS client. main use is to query the
    CaaS datastore. the search() method constructs its search from the files
    specified by elastic_path and query_config_path. these paths should be passed
    in when constructing a CaaSClient instance unless you're using it from its directory

    check the docstrings for construct_search_params() and search() methods for more info
    '''

    log_file = 'caas_client.log'
    elastic_path = '../config/elastic_search_request.json'
    query_config_path = '../config/query_config.json'

    def __init__(self, elastic_path=elastic_path, query_config_path=query_config_path, logger=None):
        self.logger = logger if logger else self._init_logger()
        self.client = self._init_client()
        self.elastic_path = elastic_path
        self.elastic_request = None
        self.query_config_path = query_config_path
        self.num_query_results = 0

    def _init_logger(self):
        with open('log.yaml', 'r') as log_conf:
            log_config = yaml.safe_load(log_conf)

        log_config['handlers']['file']['filename'] = self.log_file
        logging.config.dictConfig(log_config)
        logging.info('* * * * * * * * * * * * * * * * * * * *')
        logging.info('logging configured in client_wrapper.py')

        return logging.getLogger('caas_client')

    def _extract_json(self, json_path):
        '''utility function to extract json from a file'''
        with open(json_path) as conf:
            return json.load(conf)

    def _parse_search_response(self, query_data):
        '''return the entities from a successful query, otherwise return None'''
        self.num_query_results = int(query_data['found'])
        self.logger.info('query returned {} results'.format(self.num_query_results))
        return query_data['entities'] if self.num_query_results else None

    def _init_client(self, env='prod'):
        """env can be either 'test' or 'prod', but we'll only ever use 'prod'"""
        caas_client = client.EntityServiceClient(env)
        caas_client.x_api_key = caas_keys.CAAS_API_PROD_KEY_BRANDON  # specify our API key for the client

        return caas_client

    def _construct_search_params(self, elastic_request=None):
        '''
        query_config.json holds all search parameters other than the elasticsearch
        request. these params are: 'type', 'provider', 'follow', and 'fields'
        refer to the CaaS API documentation for an explanation of these:
        http://docs-caas.timeincapp.com/#search-and-get-examples

        these are available in case they're needed, but everything will probably
        remain static, with the exception of 'provider' which can narrow a search
        to a single brand.

        the thing that will change most often is the elastic_search_request.
        paste your request into the file 'elastic_search_request.json'
        the content to paste can be found by searching search.timeinc.com, then
        clicking on the "Advanced" tab and pasting the code from there verbatim.

        alternatively you can pass in an elasticsearch request as a python object.
        this is mainly used for debugging.
        '''
        query_config = self._extract_json(self.query_config_path)
        self.elastic_request = elastic_request if elastic_request else self._extract_json(self.elastic_path)
        search_params = {key: value for key, value in query_config.items()}
        search_params['elasticsearchRequest'] = self.elastic_request

        return search_params

    def get_batch(self, ids=[]):
        '''currently using this to get nlp results by following "$nlp_id" edges'''
        self.logger.info('querying CaaS via client.get_batch()...')

        batch_params = {
            "batchRequest": {
                "Ids": ids
            }
        }

        try:
            response = self.client.get_batch(batch_params)
        except Exception:
            self.logger.error('something went wrong...')
            self.logger.error('reraising the exception so we can look at the stack trace')
            sleep(1)
            raise

        if response.status_code == 200:
            return response.json()
        else:
            self.logger.error('query failed with response code {}'.format(response.status_code))
            self.logger.error('raising the error so we can look at it')
            response.raise_for_status()

    def search(self, elastic_request=None):
        '''
        if a specific elastic request is passed in here, it will be forwarded to
        construct_search_params. otherwise construct_search_params uses
        elastic_search_request.json to construct the parameters
        '''
        tries = 5
        success = False
        self.logger.info('querying CaaS via client.search()...')

        while not success and tries:
            try:
                search_params = self._construct_search_params(elastic_request=elastic_request)
                response = self.client.search(search_params)
                success = True
            except KeyError:
                tries -= 1
                self.logger.warning("query response doesn't have the expected keys")
                self.logger.warning('retrying search...')
            except Exception:
                self.logger.error('something went wrong...')
                self.logger.error('reraising the exception so we can look at the stack trace')
                sleep(1)
                raise

        if response.status_code == 200:
            return self._parse_search_response(response.json())
        else:
            self.logger.error('query failed with response code {}'.format(response.status_code))
            self.logger.error('raising the error so we can look at it')
            response.raise_for_status()

    def get_next_results(self):
        '''
        loop through query data by incrementing the elastic_request "from" parameter
        and calling search() for a new batch of results
        '''
        batch = self.elastic_request['from']
        next_batch = batch + self.elastic_request['size']

        if next_batch < self.num_query_results:
            self.elastic_request['from'] = next_batch
            self.logger.info('getting results for query batch starting with result {}'.format(next_batch))
            return self.search(elastic_request=self.elastic_request)
        else:
            self.logger.info('query results exhausted')
            return None


if __name__ == '__main__':
    # execute search specified in elastic_search_request.json
    caas_client = CaaSClient()
    response = caas_client.search()

    if not response:
        caas_client.logger.info('exiting...')
        sys.exit()
