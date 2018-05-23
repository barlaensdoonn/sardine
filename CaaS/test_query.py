# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# 5/21/18
# updated 5/21/18

from caas_content_client_python_3 import client
from caas_keys import CAAS_API_PROD_KEY_BRANDON


esc = client.EntityServiceClient('prod')  # this can be either 'test' or 'prod'
esc.x_api_key = CAAS_API_PROD_KEY_BRANDON  # specify our API key for the client

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

response = esc.search(search_params)
data = response.json()
data['entities'][0]  # first result returned from the query

# looping through pages, stopping at the end:
if len(data['entities'] < elastic_search_request['size']):
    pass
