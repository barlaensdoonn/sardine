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
