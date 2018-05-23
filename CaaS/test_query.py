# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# 5/21/18
# updated 5/22/18

import json
from caas_content_client_python_3 import client
from caas_keys import CAAS_API_PROD_KEY_BRANDON


elastic_search_request = {
    'size': 25,
    'from': 0,
    'query': {'match': {'_all': 'hair'}},
    'sort': [{'$date': {'unmapped_type': 'long', 'order': 'desc'}}]
}


def _extract_json(json_path):
    with open(json_path) as conf:
        return json.load(conf)


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


# looping through pages, stopping at the end:
if len(data['entities'] < elastic_search_request['size']):
    pass


if __name__ == '__main__':
    caas_client = initialize_client()
    search_params = construct_search_params(elastic_search_request)

    response = caas_client.search(search_params)
    data = response.json()
    data['entities'][0]  # first result returned from the query
