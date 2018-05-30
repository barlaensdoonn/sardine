# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# https://github.com/TimeInc/caas-content-client-python-3
# 5/21/18
# updated 5/29/18

import sys
import json
from time import sleep
import caas_keys

# add the caas python 3 client to our path so the script can use it
sys.path.insert(0, caas_keys.path_to_caas_module_home)
from caas_content_client_python_3 import client


example_elastic_search_request = {
    'size': 25,
    'from': 0,
    'query': {'match': {'_all': 'hair'}},
    'sort': [{'$date': {'unmapped_type': 'long', 'order': 'desc'}}]
}


class CaasClient:

    def __init__(self):
        self.client = self._init_client()

    def _extract_json(self, json_path):
        '''utility function to extract json from a file'''
        with open(json_path) as conf:
            return json.load(conf)

    def _parse_query_response(self, query_data):
        '''return the entities from a successful query, otherwise return None'''
        print('query returned {} results'.format(query_data['found']))
        return query_data['entities'] if query_data['found'] else None

    def _loop_thru_response(self, data):
        '''loop through response data until we reach the end'''
        if len(data['entities'] < example_elastic_search_request['size']):
            pass

    def _init_client(self, env='prod'):
        """env can be either 'test' or 'prod', but we'll only ever use 'prod'"""
        caas_client = client.EntityServiceClient(env)
        caas_client.x_api_key = caas_keys.CAAS_API_PROD_KEY_BRANDON  # specify our API key for the client

        return caas_client

    def construct_search_params(self, elastic_request=None, elastic_path='elastic_search_request.json', query_config_path='query_config.json'):
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
        query_config = self._extract_json(query_config_path)
        elastic_search_request = elastic_request if elastic_request else self._extract_json(elastic_path)
        search_params = {key: value for key, value in query_config.items()}
        search_params['elasticsearchRequest'] = elastic_search_request

        return search_params

    def search(self, elastic_request=None):
        '''
        if a specific elastic request is passed in here, it will be forwarded to
        construct_search_params. otherwise construct_search_params uses
        elastic_search_request.json to construct the parameters
        '''
        try:
            search_params = self.construct_search_params(elastic_request=elastic_request)
            response = self.client.search(search_params)
        except Exception:
            print('something went wrong...')
            print('reraising the exception so we can look at the stack trace')
            sleep(2)
            raise

        if response.status_code == 200:
            return self._parse_query_response(response.json())
        else:
            print('query failed with response code {}'.format(response.status_code))
            print('raising the error so we can look at it')
            response.raise_for_status()


if __name__ == '__main__':
    caas_client = CaasClient()
    search_params = caas_client.construct_search_params()
    response = caas_client.search(search_params)

    if not response:
        print('exiting...')
        sys.exit()
