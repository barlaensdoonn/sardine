#!/usr/local/bin/python3
# brightcove: find improperly formatted reference ids
# 5/4/17
# updated 5/5/17


import pickle
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


def print_to_file(bad_list):
    print('writing improper ids to file...')

    with open('improper_ref_ids.txt', 'w') as out:
        for item in bad_list:
            out.write('{}\n'.format(item))


def pickle_to_file(bad_list):
    print('pickling improper ids...')
    with open('improper_ref_ids_pickled.p', 'wb') as pckl:
        pickle.dump(bad_list, pckl)


if __name__ == '__main__':
    pub_id = bright_brick_road.pub_id
    bad_list = []
    offset = 0
    videos = True

    while videos:
        url = 'https://cms.api.brightcove.com/v1/accounts/{pubid}/videos?offset={offset}'.format(pubid=pub_id, offset=offset)
        r = requests.get(url, headers=get_authorization_headers())
        vid_list = r.json()

        if vid_list:
            offset += 20

            for vid in vid_list:
                ref_id = vid['reference_id']
                if 'art' in ref_id.lower():
                    id_split = ref_id.split('-')
                    if len(id_split) < 3:
                        print('found {}'.format(ref_id))
                        bad_list.append(vid['id'])

        else:
            videos = False

    print_to_file(bad_list)
    pickle_to_file(bad_list)
