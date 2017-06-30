#!/usr/local/bin/python3
# brightcove: get videos in folders
# 6/6/17
# updated 6/6/17

import sys
import requests
import pickle
import bright_brick_road
from datetime import datetime
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


def get_folders():
    '''
    map country code to Brightcove folder ids
    '''
    folders = {}
    print('getting folder dict...')

    url = 'https://cms.api.brightcove.com/v1/accounts/{}/folders'.format(pub_id)
    r = requests.get(url, headers=get_authorization_headers())

    if r.status_code == 200:
        for folder in r.json():
            folders[folder['name'].lower()] = folder['id']

        return folders
    else:
        sys.exit("couldn't get folder list, exiting script")


def get_now():
    now = datetime.now()
    return now.strftime('%m_%d_%y')


def pickle_data(data, now_str):
    with open('BC_vids_by_folder_{}.p'.format(now_str), 'wb') as pickl:
        pickle.dump(vid_dict, pickl, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    pub_id = bright_brick_road.pub_id
    folder_dict = get_folders()
    vid_dict = {}

    for key in folder_dict:
        print('getting video titles for {}...'.format(key))
        name_list = []
        offset = 0
        videos = True

        while videos:
            url = 'https://cms.api.brightcove.com/v1/accounts/{pubid}/folders/{folderid}/videos?offset={offset}'.format(pubid=pub_id, folderid=folder_dict[key], offset=offset)
            r = requests.get(url, headers=get_authorization_headers())
            vid_list = r.json()

            if vid_list:
                offset += 20

                for vid in vid_list:
                    name_list.append(vid['name'])

            else:
                vid_dict[key] = name_list
                print('got video titles for {}\n'.format(key))
                videos = False

    pickle_data(vid_dict, get_now())
