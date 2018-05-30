#!/usr/local/bin/python3
# get CaaS conent ID by URL
# 5/25/18
# updated 5/30/18

import time
import gspreadsheet
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


if __name__ == '__main__':
    brands_not_in_caas = ['http://www.bhg.com/', 'https://www.marthastewart.com',
                          'https://www.fitnessmagazine.com/', 'https://www.shape.com/']
    caas_client = CaasClient()
    g = gspreadsheet.Gsheet()
    urls_searched = 0
    urls_found = 0
    url_sources = {}

    sheet_title = 'Hair Classification Training Corpus'
    hair = g.get_spreadsheet(title=sheet_title)
    content = g.get_worksheet(hair, 'Terms/Content')

    sources = get_column_values(content, 'Content Site/Source')
    urls = get_column_values(content, 'Content URL')

    for entry in zip(sources, urls):
        if entry[1].startswith('http'):
            for brand in brands_not_in_caas:
                if brand in entry:
                    continue  # skip brands that are not currently in CaaS

                source = entry[0]
                url_sources = update_sources(url_sources, source)

                url = entry[1]
                urls_searched += 1
                print('\nsearching for {}'.format(url))
                elastic_search_request['query']['constant_score']['filter']['term']['web_article_url.raw'] = url
                response = caas_client.search(elastic_request=elastic_search_request)

                if response:
                    urls_found += 1
                    url_sources[source]['found'] += 1
                time.sleep(0.1)
