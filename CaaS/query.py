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
from utils import client_wrapper

elastic_path = 'config/elastic_search_request.json'
query_config_path = 'config/query_config.json'


def capture_args():
    '''
    capture any arguments supplied to the script on the command line and
    return the output file path if supplied
    '''
    if len(sys.argv) > 2:
        print('too many arguments')
        print('usage: python3 query.py output_file.csv')
        raise SystemExit('output_file.csv is optional')
    elif len(sys.argv) == 2:
        output_file = sys.argv[1]


if __name__ == '__main__':
    client = client_wrapper.CaaSClient(elastic_path=elastic_path, query_config_path=query_config_path)
