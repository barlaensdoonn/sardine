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


if __name__ == '__main__':
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
            source = entry[0]
            if source not in url_sources.keys():
                url_sources[source] = {
                    'searched': 0,
                    'found': 0
                }

            url = entry[1]
            urls_searched += 1
            url_sources[source]['searched'] += 1

            print('\nsearching for {}'.format(url))
            elastic_search_request['query']['constant_score']['filter']['term']['web_article_url.raw'] = url
            response = caas_client.search(caas_client.construct_search_params(elastic_request=elastic_search_request))
            if response:
                urls_found += 1
                url_sources[source]['found'] += 1
            time.sleep(0.1)
