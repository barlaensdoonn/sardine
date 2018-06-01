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


if __name__ == '__main__':
    caas_client = client_wrapper.CaaSClient(elastic_path=elastic_path, query_config_path=query_config_path)
    output_file = capture_args()

    if output_file:
        print(output_file)
    else:
        print('no output file specified')
