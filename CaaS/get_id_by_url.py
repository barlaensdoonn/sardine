#!/usr/local/bin/python3
# get CaaS conent ID by URL
# 5/25/18
# updated 5/30/18

import gspreadsheet
from time import sleep
from collections import Counter, namedtuple
from test_query import CaasClient


# example elastic search request from caas client documentation for finding content by url
elastic_search_request = {
    "size": 50,
    "query": {
        "constant_score": {
            "filter": {
                "term": {
                    "web_article_url.raw": "http://www.instyle.com/hair/find-best-hair-color-your-skin-tone"
                }
            }
        }
    }
}


def get_column_values(worksheet, column_name):
    '''get values from a specific column in a google spreadsheet'''
    title_cell = worksheet.find(column_name)
    column = title_cell.col

    return worksheet.col_values(column)


def update_sources(source_dict, source):
    if source not in url_sources.keys():
        source_dict[source] = {
            'searched': 0,
            'found': 0
        }

    source_dict[source]['searched'] += 1
    return source_dict


def _parse_ids(ids, response, key):
    '''
    type(ids) == set
    if multiple ids found, return them separated by commas, otherwise just pop
    the only element out of the set
    '''
    return ','.join(ids) if len(ids) > 1 else ids.pop()


def get_cms_ids(response):
    return {response[n]['cms_id'] for n in range(len(response)) if 'cms_id' in response[n].keys()}


def get_caas_ids(response):
    return {response[n]['$']['id'] for n in range(len(response)) if 'id' in response[n]['$'].keys()}


def update_sheet(cms_ids, caas_ids, response):
    for ids, key in zip([cms_ids, caas_ids], ['cms_id', 'caas_id']):
        if ids:
            id = _parse_ids(ids, response, key)
            if id != 'None':
                print('found {} {}'.format(key, id))
                print('updating spreadsheet...')
                content.update_cell(i, columns[key], id)
        else:
            print("didn't find any {} ids".format(key))


if __name__ == '__main__':
    Row = namedtuple('Row', ['source', 'url'])
    columns = {
        'source': 1,
        'caas_id': 2,
        'cms_id': 3,
        'url': 5
    }

    brands_not_in_caas = ['http://www.bhg.com/', 'https://www.marthastewart.com',
                          'https://www.fitnessmagazine.com/', 'https://www.shape.com/']
    caas_client = CaasClient()
    g = gspreadsheet.Gsheet()
    url_sources = {}
    search_totals = Counter()

    sheet_title = 'Hair Classification Training Corpus'
    hair = g.get_spreadsheet(title=sheet_title)
    content = g.get_worksheet(hair, 'Terms/Content')

    # iterate through the worksheet's rows
    for i in range(1, content.row_count + 1):
        row = Row(source=content.cell(i, columns['source']).value, url=content.cell(i, columns['url']).value)
        if row.url.startswith('http'):
            # skip brands that are not currently in CaaS
            if row.source in brands_not_in_caas:
                continue

            url_sources = update_sources(url_sources, row.source)
            search_totals['searched'] += 1

            print('\nsearching for {}'.format(row.url))
            elastic_search_request["query"]["constant_score"]["filter"]["term"]["web_article_url.raw"] = row.url
            response = caas_client.search(elastic_request=elastic_search_request)

            if response:
                url_sources[row.source]['found'] += 1
                search_totals['found'] += 1

                # make a set of the cms id's from the entities in the response
                cms_ids = get_cms_ids(response)
                caas_ids = get_caas_ids(response)
                update_sheet(cms_ids, caas_ids, response)

            sleep(0.01)

    print(url_sources)
    print(search_totals)
