# attempts to query legacy Time Inc's content-as-a-service (CaaS) datastore
# and output results to a csv file if an output file is specified.
# 6/1/18
# updated 6/4/18

'''
the query that is run by this script is determined by the elasticsearch request
specified in the file 'config/elastic_search_request.json'.
to find the elasticsearch request to paste into that file, type a query into the
search bar at https://search.timeinc.com/search, then click on the "Advanced" tab
and paste the code from there verbatim into the above json file.

config/query_config.json holds all search parameters other than the elasticsearch
request. these params are: 'type', 'provider', 'follow', and 'fields'.
refer to the CaaS API documentation for an explanation of these:
http://docs-caas.timeincapp.com/#search-and-get-examples
(these probably won't be changed very often)
'''

import os
import sys
import csv
import yaml
import logging
import logging.config
from collections import namedtuple
from utils import client_wrapper

elastic_path = 'config/elastic_search_request.json'
query_config_path = 'config/query_config.json'
log_file = 'query_log.log'


class QueryData:
    '''class to extract, parse, and hold the specific data we're interested in from a CaaS query'''

    # our csv writer uses these to write the header row to our csv output file
    fieldnames = ['brand', 'title', 'url', 'caas_id', 'cms_id', 'gnlp_id',
                  'wnlp_id', 'gnlp_categories', 'wnlp_categories']

    def __init__(self, query_response):
        self.response = query_response
        self.records = self._init_records()
        self._extract_caas_data()

    def _init_records(self):
        return {
            self.response[i]['$']['id']: {
                'caas_id': self.response[i]['$']['id'],
                'cms_id': '',
                'title': '',
                'url': '',
                'brand': '',
                'gnlp_id': '',
                'wnlp_id': '',
                'gnlp_categories': {},
                'wnlp_categories': {}
            } for i in range(len(self.response))
        }

    def _extract_caas_data(self):
        '''
        extract data from a caas record

        some currently unextracted parameters:
        taxonomy = entry['si']['taxonomy']['tags']  # si (sports illustrated) specific
        type = entry['$type'][0]['$id'].split('/')[1]
        url_alt = entry['asset_url']
        pronto_data = entry['pronto']['article']
        content = entry['web_article_content']
        '''

        for i in range(len(self.response)):
            entry = self.response[i]
            caas_id = entry['$']['id']
            self.records[caas_id]['cms_id'] = entry['cms_id'] if 'cms_id' in entry.keys() else None
            self.records[caas_id]['title'] = entry['web_article_title'].strip() if 'web_article_title' in entry.keys() \
                else entry['$name'] if '$name' in entry.keys() else None
            self.records[caas_id]['url'] = entry['web_article_url'] if 'web_article_url' in entry.keys() else None
            self.records[caas_id]['brand'] = entry['brand'] if 'brand' in entry.keys() else None
            self.records[caas_id]['gnlp_id'] = entry["$i_nlp_source_google"][0]['$id'] if "$i_nlp_source_google" in entry.keys() else None
            self.records[caas_id]['wnlp_id'] = entry["$i_nlp_source_watson"][0]['$id'] if "$i_nlp_source_watson" in entry.keys() else None

    def _extract_nlp_categories(self, nlp_records, nlp_data, type):
        '''any nlp results that do not contain the field 'nlp_categories' are dropped'''
        for i in range(len(nlp_data)):
            nlp_id = nlp_data[i]['$']['id']
            if 'nlp_categories' in nlp_data[i].keys():
                if type is 'google':
                    nlp_records[nlp_id].categories['name'] = nlp_data[i]['nlp_categories'][0]['name']
                    nlp_records[nlp_id].categories['confidence'] = nlp_data[i]['nlp_categories'][0]['confidence']
                else:
                    nlp_records[nlp_id].categories['label'] = nlp_data[i]['nlp_categories'][0]['label']
                    nlp_records[nlp_id].categories['score'] = nlp_data[i]['nlp_categories'][0]['score']

        # drop any records that don't have categories
        nlp_records = {key: value for key, value in nlp_records.items() if value.categories}
        logger.info('extracted {} nlp categories for {} records'.format(type, len(nlp_records)))

        return nlp_records

    def _update_records_with_nlp_data(self, nlp_records, type):
        '''update self.records with the extracted nlp data'''
        cat_key = 'gnlp_categories' if type is 'google' else 'wnlp_categories'
        for key in nlp_records.keys():
            caas_id = nlp_records[key].caas_id
            self.records[caas_id][cat_key] = nlp_records[key].categories

    def filter_out_empties(self):
        self.records = {key: value for key, value in self.records.items() if value['url']}

    def filter_out_training_urls(self, dupes):
        logger.warning('found duplicate urls in the training set, dropping them from the records')
        self.records = {key: value for key, value in self.records.items() if value['url'] not in dupes}

    def get_nlp_data(self, client, type='google'):
        '''
        construct a dict formatted as nlp_id: nlp(caas_id, {}), where the value of
        nlp_id is a namedtuple called nlp that has 2 fields: caas_id and categories.
        then query CaaS for google or watson NLP data for the entries in records arg
        that have associated nlp ids. type kwarg should be either 'google' or 'watson'.

        currently this function is capturing 'nlp_categories' and their corresponding
        'confidence' level (google) or score (watson).

        other available fields for google are 'nlp_entities' and 'nlp_docSentiment'.
        other available fields for watson are 'nlp_keywords', 'nlp_concepts',
        'nlp_doc_sentiment', and 'nlp_entities'.
        '''
        logger.info('getting available {} nlp data for this batch of query data'.format(type))
        Nlp = namedtuple('Nlp', ['caas_id', 'categories'])
        id_key = 'gnlp_id' if type is 'google' else 'wnlp_id'

        nlp_records = {self.records[key][id_key]: Nlp(key, {}) for key in self.records.keys() if self.records[key][id_key]}
        nlp_ids = [key for key in nlp_records.keys()]
        nlp_data = client.get_batch(ids=nlp_ids)
        logger.info('query returned {} results'.format(len(nlp_data)))

        nlp_records = self._extract_nlp_categories(nlp_records, nlp_data, type)
        self._update_records_with_nlp_data(nlp_records, type)


def _initialize_logger(name):
    logger = logging.getLogger(name)
    logger.info('{} logger instantiated'.format(name))
    return logger


def _init_output_file(output_file):
    '''
    create a new csv file and write headers to it. headers are taken from the
    QueryData "fieldnames" class variable
    '''
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=QueryData.fieldnames)
        writer.writeheader()

    logger.info('{} file initialized with headers'.format(output_file))
    return output_file


def _check_output(output_file):
    '''
    if output file exists, confirm overwrite or append. if append, return it.
    if overwrite or it doesn't exist initialize it with headers, then return it.
    '''
    if os.path.isfile(output_file):
        logger.info('the specified output file already exists, do you want to overwrite (o) or append (a) to it ?')
        mode = input('o/a: ').lower()
        if mode.lower().startswith('o'):
            return _init_output_file(output_file)
        else:
            return output_file

    return _init_output_file(output_file)


def configure_logger():
    with open('utils/log.yaml', 'r') as log_conf:
        log_config = yaml.safe_load(log_conf)

    log_config['handlers']['file']['filename'] = log_file
    logging.config.dictConfig(log_config)
    logging.info('* * * * * * * * * * * * * * * * * * * *')
    logging.info('logging configured')

    return _initialize_logger('query')


def capture_args():
    '''
    capture any arguments supplied to the script on the command line. if an output file
    is specified, check if it already exists and confirm overwrite/append if it does.
    return None if no output is specified, so we know not to write results.
    '''
    if len(sys.argv) > 2:
        logger.error('too many arguments')
        logger.info('usage: python3 query.py output_file.csv')
        raise SystemExit('output_file.csv is optional')
    elif len(sys.argv) == 2:
        output = sys.argv[1]
        output = os.path.join('output', output)
        return _check_output(output)
    else:
        return None


def drop_dupes(caas_id_set, data):
    for key in data.keys():
        caas_id = data[key]['caas_id']
        if caas_id in caas_id_set:
            del data[caas_id]
        else:
            caas_id_set.add(caas_id)


def write_to_file(outfile, records):
    '''append records to csv file'''
    logger.info('writing {} records to {}'.format(len(records), outfile))

    with open(outfile, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=QueryData.fieldnames)
        for key in records.keys():
            writer.writerow(records[key])


def get_existing_urls():
    '''this is a one time function to get duplicates from an existing file'''
    import pandas

    csv_file = 'utils/Hair_Classification_Training_Corpus_Content_as_of_06262018.csv'
    colnames = ['content_source', 'caas_id', 'cms_id', 'content_url', 'content_type', 'content_title']
    existing = pandas.read_csv(csv_file, names=colnames)

    return existing.content_url.tolist()


if __name__ == '__main__':
    # initialize our logger and check to see if an output file was passed in on the command line
    logger = configure_logger()
    output = capture_args()

    # get existing urls from a copy of Hair Training Classification Corpus spreadsheet
    training_urls = get_existing_urls()

    # initialize our caas_client, which is a wrapper Brandon wrote around Time Inc's
    # caas-python-3-client that makes it a little easier for us to query the CaaS datastore.
    # this wrapper lives in the utils/client_wrapper.py module
    caas_client = client_wrapper.CaaSClient(elastic_path=elastic_path,
                                            query_config_path=query_config_path,
                                            logger=_initialize_logger('caas_client'))

    # conduct an initial search to see how many results are returned
    # from the query specified in elastic_search_request.json
    response = caas_client.search()

    # we don't need to move past the initial search if we're not outputting to a file
    if output:
        caas_ids = set()
        # caas_client.search() returns a fixed # of results specified in the
        # elasticsearch request's "size" param. if we are outputting to a file,
        # we need to extract the data from the current response, write it to our file,
        # then repeat with the next batch til we're done.
        while response:
            data = QueryData(response)
            drop_dupes(caas_ids, data)

            # query CaaS for nlp data if it's available (technically this follows $nlp_id edges)
            for type in ['google', 'watson']:
                data.get_nlp_data(caas_client, type=type)

            # drop any records that don't have a url, or that already exist in the training set spreadsheet
            data.filter_out_empties()
            data.filter_out_training_urls(training_urls)

            # attempt to append this batch to our file. skip this batch
            # if we get a unicode error which happens occasionally on windows
            try:
                write_to_file(output, data.records)
            except UnicodeEncodeError as e:
                logger.warning('UnicodeEncodeError encountered when trying to write to file, skipping this batch...')

            logger.info(' - - - - - - - - - - - - - - - - - - - ')
            response = caas_client.get_next_results()
