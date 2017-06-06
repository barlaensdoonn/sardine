#!/usr/local/bin/python3
# brightcove: reformat improperly formatted reference ids
# 5/4/17
# updated 5/5/17


import json
import pandas
import requests
import bright_brick_road
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_authorization_headers():
    '''
    convenience method that obtains an OAuth access token and embeds it
    appropriately in a map suitable for use with the requests HTTP library
    '''
    client_id = bright_brick_road.client_id
    client_secret = bright_brick_road.secret
    access_token_url = "https://oauth.brightcove.com/v3/access_token"
    # profiles_base_url = "http://ingestion.api.brightcove.com/v1/accounts/{pubid}/profiles".format(pubid=pub_id)
    access_token = None

    r = requests.post(access_token_url, params="grant_type=client_credentials", auth=(client_id, client_secret), verify=False)

    if r.status_code == 200:
        access_token = r.json().get('access_token')
    else:
        print('could not get authorization headers')

    return {'Authorization': 'Bearer ' + access_token, "Content-Type": "application/json"}


def handle_commas(ref_id):
    id_split = ref_id.split(',')
    id_split = [item.strip() for item in id_split[:]]
    id_join = ', '.join(id_split)

    return id_join


def handle_arts(ref_id):
    id_split = ref_id.split('-')

    if len(id_split) < 3:
        fields = [id_split[0][:-3], 'ART', id_split[-1]]
        id_join = '-'.join(fields)
        return id_join
    else:
        return ref_id


if __name__ == '__main__':
    pub_id = bright_brick_road.pub_id
    reformats = {}
    offset = 0
    videos = True

    while videos:
        url = 'https://cms.api.brightcove.com/v1/accounts/{pubid}/videos?offset={offset}'.format(pubid=pub_id, offset=offset)
        r = requests.get(url, headers=get_authorization_headers())
        vid_list = r.json()

        if vid_list:
            offset += 20

            for vid in vid_list:
                new_id = None
                ref_id = vid['reference_id']

                if ',' in ref_id:
                    new_id = handle_commas(ref_id)
                elif 'art' in ref_id.lower():
                    new_id = handle_arts(ref_id)

                if new_id and ref_id != new_id:
                    reformats[vid['id']] = {'old': ref_id, 'new': new_id}

        else:
            videos = False

    for key in reformats.keys():
        url = 'https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{vid_id}'.format(pubid=pub_id, vid_id=key)
        data = {'reference_id': reformats[key]['new']}
        json_data = json.dumps(data)
        r = requests.patch(url, headers=get_authorization_headers(), data=json_data)

        if r.status_code == 200:
            print('successfully changed: {}'.format(key))
        elif r.status_code == 409:
            print('could not change: {}'.format(key))
            print('already in use: {}'.format(reformats[key]['new']))
        else:
            print('something went wrong with {}'.format(key))
            print('status_code: {}'.format(r.status_code))

    # find ids in reformats dict in BC_ids_in_use list from devs
    used = pandas.read_csv('ignore/BC_ids_in_use_May_02.csv')
    unused = pandas.read_csv('ignore/BC_ids_not_in_use_May_02.csv')

    id_list = []
    for key in reformats.keys():
        id_list.append(key)

    ids_in_use = []
    for value in used['ids_in_use']:
        if str(value) in id_list:
            ids_in_use.append(str(value))
