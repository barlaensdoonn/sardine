#!/usr/local/bin/python3
# brightcove playground
# 3/30/17

import os
import json
import requests
import logging
import logging.config
import bright_brick_road
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


filepath = '/Volumes/MACKEREL/Oven/Localization/Plank_smoked_salmon/Exports/localizedVP9/Plank_smoked_salmon_UK.webm'


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
        self.id = None
        self.json = None
        self.upload_url = None
        self.ingest_url = None


class Brightcove(object):
    '''methods for interacting with Brightcove CMS and DI APIs'''

    def __init__(self):
        logger.info('* * * * * * * * * * * * * * * * * * * *')
        logger.info('initializing brightcove class...')
        self.pub_id = bright_brick_road.pub_id
        self.folders = self._get_folders()

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
        else:
            logger.error('unable to get acces token from brightcove')
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)

        return {'Authorization': 'Bearer ' + access_token, "Content-Type": "application/json"}

    def _get_folders(self):
        '''
        map country code to Brightcove folder ids
        '''
        folders = {}

        url = 'https://cms.api.brightcove.com/v1/accounts/{}/folders'.format(self.pub_id)
        r = requests.get(url, headers=self._get_authorization_headers())

        if r.status_code == 200:
            logger.info('got folder list')
        else:
            logger.error('unable to get folder list')
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)

        for folder in r.json():
            folders[folder['name'].lower()] = folder['id']

        return folders

    def search_for_video(self, ref_id):
        '''
        CMS API call to search for existing video by reference id
        '''
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/ref:{refid}".format(pubid=self.pub_id, refid=ref_id)
        r = requests.get(url, headers=self._get_authorization_headers())

        if r.status_code == 200:
            found_vid = r.json()
            logger.info('reference id {} exists as "{}" [original filename: {}]'.format(ref_id, found_vid['name'], found_vid['original_filename']))
            video.id = found_vid['id']
            video.json = found_vid
            return found_vid
        else:
            logger.info('video with reference_id {} does not exist'.format(ref_id))
            return None

    def create_video(self, video):
        '''
        CMS API call to create a video in the VideoCloud catalog
        '''
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/".format(pubid=self.pub_id)
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

        if r.status_code == 201:
            vid_deets = r.json()
            video.json = vid_deets
            video.id = vid_deets['id']
            logger.info('created video object "{}"'.format(video.name))
        else:
            logger.error('unable to create video object "{}"'.format(video.name))

    def move_to_folder(self, video):
        '''
        CMS API call to put video in corresponding country folder
        '''
        folder_id = self.folders[video.country]
        url = 'https://cms.api.brightcove.com/v1/accounts/{pubid}/folders/{folderid}/videos/{videoid}'.format(pubid=self.pub_id, folderid=folder_id, videoid=video.id)
        r = requests.put(url, headers=self._get_authorization_headers())

        if r.status_code == 204:
            logger.info('moved {} into {} folder'.format(video.name, video.country.upper()))
        else:
            logger.error('unable to move {} into {} folder'.format(video.name, video.country.upper()))
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)

    def delete_video(self, video):
        '''
        CMS API call to delete a video in the VideoCloud catalog
        '''
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}".format(pubid=self.pub_id, videoid=video.id)
        r = requests.delete(url, headers=self._get_authorization_headers())

        if r.status_code == 204:
            logger.info('{} was deleted'.format(video.name))
        else:
            logger.error('unable to delete {}'.format(video.name))
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)

    def upload(self, video):
        '''
        performs an authenticated request to discover a brightcove-provided
        location to securely upload a source file
        '''
        # obtain a file upload location
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/upload-urls/{sourcefilename}".format(pubid=self.pub_id, videoid=video.id, sourcefilename=video.name)
        r = requests.get(url, headers=self._get_authorization_headers())
        upload_urls_response = r.json()

        video.upload_url = upload_urls_response['signed_url']
        video.ingest_url = upload_urls_response['api_request_url']

        if r.status_code == 200:
            logger.info('received upload url for {}'.format(video.filename))
        else:
            logger.error('did not receive upload url for {}'.format(video.filename))
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)

        # Upload the contents of our local file to the location provided via HTTP PUT
        # This is not recommended for large files
        with open(video.path, 'rb') as fh:
            logger.info('uploading...')
            s = requests.put(video.upload_url, data=fh.read())

        if s.status_code == 200:
            logger.info('{} uploaded'.format(video.filename))
        else:
            logger.error('unable to upload {}'.format(video.filename))
            logger.error('status code: {}, reason: {}'.format(s.status_code, s.reason))
            logger.error(s.text)

    def di_request(self, video):
        '''
        Ingest API call to populate a video with transcoded renditions
        from a remotely accessible source asset
        '''
        url = "https://ingest.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/ingest-requests".format(pubid=self.pub_id, videoid=video.id)

        data = {
            "master": {
                "url": '{}'.format(video.ingest_url)
            },
            "profile": "videocloud-default-v1"
        }

        json_data = json.dumps(data)
        r = requests.post(url, headers=self._get_authorization_headers(), data=json_data)

        if r.status_code == 200:
            logger.info('{} ingested'.format(video.filename))
        else:
            logger.error('unable to ingest {}'.format(video.filename))
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)


if __name__ == '__main__':
    logging.config.fileConfig('log.conf')
    logger = logging.getLogger('log')
    logger.info('* * * * * * * * * * * * * * * * * * * * \n')

    video = Video(filepath)
    brightcove = Brightcove()

    search = brightcove.search_for_video(video.reference_id)

    if not search:
        brightcove.create_video(video)
        brightcove.move_to_folder(video)
        brightcove.upload(video)
        brightcove.di_request(video)
    else:
        logger.info('{} found, moving on...'.format(video.filename))
