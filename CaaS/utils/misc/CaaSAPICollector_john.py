
# coding: utf-8

# In[ ]:

from caas_content_client_python_2_7 import client

import sys
sys.path.append('/Users/jgugliotti1271/Documents/L1DEV/development/ACDS_BDBAnalysis/automation/scoring')
import redshift, time
import json
import csv
from datetime import datetime, timedelta
from hashlib import md5

entity_service_client = client.EntityServiceClient('prod')
entity_service_client.x_api_key = CAAS_API_KEY_PROD


def collect_content_urls(toplevel_domain='instyle.com', historical_day_count=90): 
    days = historical_day_count
    dba = redshift.DBAccess()
    unique_urls = set()
    local_url_dump = open('instyle_urls.txt','wb')
    cursor = dba.get_cursor()
    sql = """select clean_url from _360.ti_cookie_v6_%Y_%m_%d where "domain" = '""" + toplevel_domain + """' group by clean_url having count(*) > 35"""
    for d in range(1,days):
        extract_date = datetime.now() - timedelta(days = d)
        cursor.execute(extract_date.strftime(sql))
        for res in cursor.fetchall():
            if res[0] not in unique_urls:
                unique_urls.add(res[0])
        print datetime.now()
        print len(unique_urls)
        open(toplevel_domain + 'urls.txt','wb').write("\n".join(list(unique_urls)))
    return unique_urls

def collect_content_urls_n(unique_urls=set(), processed_urls=set(), toplevel_domain='instyle.com', start=datetime.now(), end = datetime.now()): 
    
    dba = redshift.DBAccess()
    unique_urls = set()
    local_url_dump = open('instyle_urls.txt','wb')
    cursor = dba.get_cursor()
    sql = """select clean_url from _360.ti_cookie_v6_%Y_%m_%d where "domain" = '""" + toplevel_domain + """' group by clean_url having count(*) > 35"""
    while start <= end:
        
        cursor.execute(start.strftime(sql))
        for res in cursor.fetchall():
            if res[0] not in processed_urls:
                unique_urls.add(res[0])
        print datetime.now(), len(unique_urls)
        open(toplevel_domain + 'urls_extended.txt','wb').write("\n".join(list(unique_urls)))
        start += timedelta(days=1)
    return unique_urls

fin  = open('instyle_urls.txt','rbU')
reader=csv.reader(fin)
processed_urls = set([r[0] for r in reader])
fin.close()


unique_urls = set()
#print datetime.now()
#unique_urls = collect_content_urls_n(unique_urls,processed_urls,start = datetime(2017,2,1), end = datetime(2018,5,10) )
#print datetime.now()
fin = open('instyle.comurls_extended.txt','rbU')
reader=csv.reader(fin)
for r in reader: unique_urls.add(r[0])
print len(unique_urls) , 'located'


#unique_urls = collect_content_urls_n(set(unique_urls), start = datetime(2017,2,9), end = datetime(2018,2,10))

# In[ ]:

unique_urls = list(unique_urls)
print len(unique_urls)



#url = None
def get_caas_record(url):
    
    
    elastic_search_request = {
            'size': 50,
            'query': {
                'constant_score': {
                    'filter': {
                        'term': {
                            'web_article_url.raw': url
                        }
                    }
                }
        }
    }    
    #print json.dumps(elastic_search_request, indent = 2) 
    search_parameters = {'elasticsearchRequest': elastic_search_request, 
                       'type': 'web_article'
                        ,'provider': 'tardisnyc'
                        ,'follow' : []
                        , "fields" : None}
    results = entity_service_client.search(search_parameters)
    ##print 'calling at ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #print results.status_code
    
    try:
        return results.json()
    except:
        return None 

def extract_caas_record(caas_data):
    if not 'entities' in caas_data:
        return None
    entities = caas_data['entities']
    objects = {}
    for e in entities:
        content = e['web_article_content'] if 'web_article_content' in e else None
        url = e['asset_url'] if 'asset_url' in e else None
        google_nlp_uid = e["$i_nlp_source_google"][0]['$id'] if "$i_nlp_source_google" in e else None
        watson_nlp_id = e["$i_nlp_source_watson"] if "$i_nlp_source_watson" in e else None
        objects[url] = {
            "url": url
            , "content": content
            , "google_nlp_uid": google_nlp_uid
            , "watson_nlp_id": watson_nlp_id            
        }

    return objects

## collect NLP data using 

def get_google_nlp_results(google_nlp_source_id):
    if google_nlp_source_id is None:
        return None
    #print 'getting results for ' + google_nlp_source_id
    get_batch_parameters = {
                            'batchRequest':  {
                                              'Ids': [
                                                google_nlp_source_id
                                              ]
                                             }
                           }
    google_nlp_results = entity_service_client.get_batch(get_batch_parameters)
    try: 
        google_nlp_results = google_nlp_results.json()
    except:
        google_nlp_results = None
    
    return google_nlp_results

def enrich_cass_record_stored_annotations(cass_json):
    google_nlp_uid = cass_json['google_nlp_uid']

    annotations = get_google_nlp_results(google_nlp_uid)
    
    
    if annotations:
        a= annotations[0]
        cass_json['google_nlp_entities'] = a['nlp_entities'] if 'nlp_entities' in a else None
        cass_json['google_nlp_categories'] = a['nlp_categories'] if 'nlp_categories' in a else None
        cass_json['google_nlp_docSentiment'] = a['nlp_docSentiment'] if 'nlp_docSentiment' in a else None

    
def make_clean_url_md5(url):
    if not url:
        return None
    url = url.replace('https://','')
    return md5(url).hexdigest()

def add_hash_to_record(recobj):
    hashval = make_clean_url_md5(recobj['url'])
    recobj['url_md5'] = hashval
    if not hashval:
        return False
    else:
        return True

    

url='https://www.coastalliving.com/travel/rentals/bali-villa-vacation-rental'
print 'ssss'
def iterate(unique_urls, skip = 296):
    
    #print 'skipping ', skip

    for i in range(skip,len(unique_urls)):
        url = unique_urls[i]
        if url is None or len(url.strip()) == 0:
            print 'no url'
            continue
        #print url
        protocol = 'http://'
        www = "www."
        if 'http://www.' not in url:
            formatted_url = protocol + www + url
        else:
            formatted_url = url

        
        
        print i,  'collect', formatted_url
        
        data = collect_data(formatted_url)

def collect_data(url):

    s = datetime.now()
    caas_data = get_caas_record(url)
    #print caas_data['found']
    e = datetime.now()
    if (e-s).microseconds*10**-6 < .200:
        time.sleep(.05 + (.2 - (e-s).microseconds*10**-6))
        print 'sleep', .2 - (e-s).microseconds*10**-6  
    caas_records = extract_caas_record(caas_data)
    
    if not caas_records:
        return None

    
    for caas_record_url in caas_records:
        s = datetime.now()
        enrich_cass_record_stored_annotations(caas_records[caas_record_url])
        e = datetime.now()

        if (e-s).microseconds*10**-6 < .200:
            time.sleep(.05 + (.2 - (e-s).microseconds*10**-6))
            print 'sleep', .2 - (e-s).microseconds*10**-6        
            
        if add_hash_to_record(caas_records[caas_record_url]): 
            print 'write ', 'content_and_tags/' + caas_records[caas_record_url]['url_md5'] + ".json"
            #print caas_records[caas_record_url].keys()
            open('content_and_tags/' + caas_records[caas_record_url]['url_md5'] + ".json",'wb').write(json.dumps(caas_records[caas_record_url]))
    return caas_records


iterate(unique_urls,skip=20545)

