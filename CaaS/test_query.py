# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# 5/21/18
# updated 5/22/18

import sys
import json
from time import sleep
from caas_content_client_python_3 import client
from caas_keys import CAAS_API_PROD_KEY_BRANDON


elastic_search_request = {
    'size': 25,
    'from': 0,
    'query': {'match': {'_all': 'hhistry'}},
    'sort': [{'$date': {'unmapped_type': 'long', 'order': 'desc'}}]
}


def _extract_json(json_path):
    with open(json_path) as conf:
        return json.load(conf)


def _parse_query_response(query_data):
    print('query returned {} results'.format(query_data['found']))
    return query_data['entities'] if query_data['found'] else None


def _loop_thru_response(data):
    '''loop through response data until we reach the end'''
    if len(data['entities'] < elastic_search_request['size']):
        pass


def initialize_client(env='prod'):
    """env can be either 'test' or 'prod', but we'll only ever use 'prod'"""

    caas_client = client.EntityServiceClient(env)
    caas_client.x_api_key = CAAS_API_PROD_KEY_BRANDON  # specify our API key for the client

    return caas_client


def construct_search_params(elastic_search_request, query_config_path='query_config.json'):
    '''
    query_config.json holds all search parameters other than the elasticsearch request.
    these params are: 'type', 'provider', 'follow', and 'fields'
    refer to the CaaS API documentation for an explanation of these:
    http://docs-caas.timeincapp.com/#search-and-get-examples

    these are available in case they're needed, but everything will probably
    remain static, with the exception of 'provider' which can narrow a search
    to a single brand.
    '''
    query_config = _extract_json(query_config_path)
    search_params = {key: value for key, value in query_config.items()}
    search_params['elasticsearchRequest'] = elastic_search_request

    return search_params


def search(search_params):
    try:
        response = caas_client.search(search_params)
    except Exception:
        print('something went wrong...')
        print('reraising the exception so we can look at the stack trace')
        sleep(2)
        raise

    if response.status_code == 200:
        return _parse_query_response(response.json())
    else:
        print('query failed with response code {}'.format(response.status_code))
        print('raising the error so we can look at it')
        response.raise_for_status()


if __name__ == '__main__':
    caas_client = initialize_client()
    search_params = construct_search_params(elastic_search_request)
    response = search(search_params)

    if not response:
        print('exiting...')
        sys.exit()
