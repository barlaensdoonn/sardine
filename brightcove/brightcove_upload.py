#!/usr/local/bin/python3
# brightcove playground
# 3/30/17

import os
import json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import bright_brick_road

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


filepath = '/Volumes/MACKEREL/Oven/Localization/Plank_smoked_salmon/Exports/Plank_smoked_salmon_UK.mp4'


class Video(object):
    '''holds video info for Brightcove'''

    def __init__(self, path):
        self.path = path
        self.filename = os.path.split(self.path)[-1]
        self.name = os.path.splitext(self.filename)[0]
        self.country = self.name[-2:].lower()
        self.source_id = '30283-AU'
        self.reference_id = '42304-UK'
        self.recipe_url = 'http://allrecipes.co.uk/recipe/42304/plank-smoked-salmon.aspx'
        self.state = 'INACTIVE'
        self.music_track = 'Sugar Zone'
        self.music_track_author = 'Silent Partner'
        self.music_track_url = 'https://www.youtube.com/audiolibrary/music'
        self.published_date = None
        self.youtube_url = None
        self.brightcove_id = None


class Brightcove(object):
    '''methods for interacting with Brightcove CMS and DI APIs'''

    def __init__(self):
        self.pub_id = bright_brick_road.pub_id
        self.folders = {}

    def _get_authorization_headers(self):
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

    def search_for_video(self, ref_id):
        '''
        CMS API call to search for existing video by reference id
        '''
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/ref:{refid}".format(pubid=self.pub_id, refid=ref_id)
        r = requests.get(url, headers=self._get_authorization_headers())

        if r.status_code == 200:
            return r.json()
        else:
            return None

    def create_video(self, video):
        '''
        CMS API call to create a video in the VideoCloud catalog
        '''
        url = ("https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/").format(pubid=self.pub_id)
        data = {
            'name': video.name,
            'state': video.state,
            'reference_id': video.reference_id,
            'tags': [video.country],
            'custom_fields': {
                'sourceid': video.source_id,
                'musictrack': video.music_track,
                'musictrackauthor': video.music_track_author,
                'musictrackurl': video.music_track_url,
                'filename': video.filename,
                # 'publisheddate': videoFromFile.PublishedDate,
                # 'ytvideoUrl': videoFromFile.YTVideoURL
            },
            'link': {
                'url': video.recipe_url,
                'text': '',
            }
        }

        json_data = json.dumps(data)
        r = requests.post(url, headers=self._get_authorization_headers(), data=json_data)

        vid_deets = r.json()
        video.brightcove_id = vid_deets['id']

        return vid_deets

    def delete_video(self, video_id):
        '''
        CMS API call to delete a video in the VideoCloud catalog
        '''
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}".format(pubid=self.pub_id, videoid=video_id)
        r = requests.delete(url, headers=self._get_authorization_headers())

        return r.status_code

    def upload(self, video_id, filepath, source_filename):
        '''
        performs an authenticated request to discover a Brightcove-provided location
        to securely upload a source file
        '''
        # Perform an authorized request to obtain a file upload location
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/upload-urls/{sourcefilename}".format(pubid=self.pub_id, videoid=video_id, sourcefilename=source_filename)
        r = requests.get(url, headers=self._get_authorization_headers())
        upload_urls_response = r.json()

        # Upload the contents of our local file to the location provided via HTTP PUT
        # This is not recommended for large files
        with open(filepath, 'rb') as fh:
            s = requests.put(upload_urls_response['signed_url'], data=fh.read())

        return (upload_urls_response, s.status_code)

    def di_request(self, video_id, upload_urls_response):
        '''
        Ingest API call to populate a video with transcoded renditions
        from a remotely accessible source asset
        '''
        url = "https://ingest.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/ingest-requests".format(pubid=self.pub_id, videoid=video_id)

        data = {
            "master": {
                "url": '{}'.format(upload_urls_response['api_request_url'])
            },
            "profile": "videocloud-default-v1"
        }

        json_data = json.dumps(data)
        r = requests.post(url, headers=self._get_authorization_headers(), data=json_data)

        return r.status_code


if __name__ == '__main__':
    video = Video(filepath)
    brightcove = Brightcove()

    vid = brightcove.create_video(video)
    upload_response = brightcove.upload(video.brightcove_id, video.path, video.filename)
    brightcove.di_request(video.brightcove_id, upload_response[0])
