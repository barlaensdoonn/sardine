#!/usr/local/bin/python3
# brightcove playground
# 3/30/17

import json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import bright_brick_road

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


pub_id = bright_brick_road.pub_id
filepath = '/Volumes/MACKEREL/Oven/Localization/Plank_smoked_salmon/Exports/Plank_smoked_salmon_UK.mp4'
source_filename = 'Plank_smoked_salmon_UK'


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

    return {'Authorization': 'Bearer ' + access_token, "Content-Type": "application/json"}


def create_video():
    '''
    CMS API call to create a video in the VideoCloud catalog
    '''
    url = ("https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/").format(pubid=pub_id)
    data = '''{
        "name": "TEST_VIDEO",
        "state": "INACTIVE"
    }'''
    r = requests.post(url, headers=get_authorization_headers(), data=data)

    return r.json()


def delete_video(video_id):
    '''
    CMS API call to delete a video in the VideoCloud catalog
    '''
    url = ("https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}").format(pubid=pub_id, videoid=video_id)
    r = requests.delete(url, headers=get_authorization_headers())

    return r.status_code


def upload(video_id, filepath, source_filename):
    '''
    performs an authenticated request to discover a Brightcove-provided location
    to securely upload a source file
    '''
    # Perform an authorized request to obtain a file upload location
    url = ("https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/upload-urls/{sourcefilename}").format(pubid=pub_id, videoid=video_id, sourcefilename=source_filename)
    r = requests.get(url, headers=get_authorization_headers())
    upload_urls_response = r.json()

    # Upload the contents of our local file to the location provided via HTTP PUT
    # This is not recommended for large files
    with open(filepath, 'rb') as fh:
        s = requests.put(upload_urls_response['signed_url'], data=fh.read())

    return {'upload_urls_response': upload_urls_response, 'upload_request_response': s.status_code}


def di_request(video_id, upload_urls_response):
    '''
    Ingest API call to populate a video with transcoded renditions
    from a remotely accessible source asset
    '''
    url = ("https://ingest.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/ingest-requests").format(pubid=pub_id, videoid=video_id)

    data = {
        "master": {
            "url": '{}'.format(upload_urls_response['api_request_url'])
        },
        "profile": "videocloud-default-v1"
    }

    json_data = json.dumps(data)
    r = requests.post(url, headers=get_authorization_headers(), data=json_data)

    return r.status_code


if __name__ == '__main__':
    vid = create_video()
    upload_response = upload(vid['id'], filepath, source_filename)
    di_request(vid['id'], upload_response['upload_urls_response'])
