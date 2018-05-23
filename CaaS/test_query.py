# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# 5/21/18
# updated 5/22/18

import json
from caas_content_client_python_3 import client


def get_client_config(config_path='caas_client_config.json'):
    with open(config_path) as confp:
        return json.load(confp)


def initialize_client(config, env='prod'):
    """env can be either 'test' or 'prod', but we'll only ever use 'prod'"""

    caas_client = client.EntityServiceClient('prod')
    caas_client.x_api_key = config['CAAS_API_PROD_KEY']  # specify our API key for the client

    return caas_client


elastic_search_request = {
    'size': 25,
    'from': 0,
    'query': {'match': {'_all': 'hair'}},
    'sort': [{'$date': {'unmapped_type': 'long', 'order': 'desc'}}]
}

search_params = {
    'elasticsearchRequest': elastic_search_request,
    'type': 'web_article'
}


# looping through pages, stopping at the end:
if len(data['entities'] < elastic_search_request['size']):
    pass


if __name__ == '__main__':
    client_config = get_client_config()
    caas_client = initialize_client()

    response = caas_client.search(search_params)
    data = response.json()
    data['entities'][0]  # first result returned from the query
