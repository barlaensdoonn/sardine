#!/usr/local/bin/python3
# brightcove playground
# 3/30/17
# updated 4/5/17

import os
import sys
import json
import shutil
import requests
import gspread
import logging
import logging.config
import bright_brick_road
from oauth2client.service_account import ServiceAccountCredentials
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


search_path = bright_brick_road.search_path
uploaded_path = bright_brick_road.uploaded


class Video(object):
    '''holds video info for input into Brightcove class'''

    stills_path = bright_brick_road.stills_base_path

    def __init__(self, filename, music_list, source_id_dict):
        '''
        self.paths['uploaded'] acts as a flag:
        if the video was uploaded successfully, the path is made in brightcove.upload(),
        and will evaluate to True in video.move(); if upload was unsuccessful,
        it stays as None and evaluates to False, and video is not moved
        '''
        self.filename = filename
        self.name = os.path.splitext(self.filename)[0]
        self.vid_name = self.name[0:-3]
        self.vid_name_compare = self.vid_name.replace('_', ' ').lower()
        self.country = self.name[-2:].lower()
        self.source_id = None
        self.reference_id = '42304-UK'
        self.state = 'INACTIVE'
        self.music_track = None
        self.music_track_author = None
        self.id = None
        self.json = None

        self._get_stills_paths()
        self._get_music_info(music_list)
        self._get_source_ids(source_id_dict)

        self.paths = {
            'source': os.path.abspath(filename),
            'poster': None,
            'thumbnail': None,
            'uploaded': None
        }

        self.urls = {
            'music_track': None,
            'recipe': 'http://allrecipes.co.uk/recipe/42304/plank-smoked-salmon.aspx',
            'upload': {
                'video': None,
                'poster': None,
                'thumbnail': None
            },
            'ingest': {
                'video': None,
                'poster': None,
                'thumbnail': None
            }
        }

    def _get_stills_paths(self):
        '''
        utility function to find stills to use for DI API poster and thumbnail images
        '''
        still_path = Video.stills_path

        for dirpath, dirnames, filenames in os.walk(still_path):
            for thing in filenames:
                split = os.path.splitext(thing)

                if split[0].lower().endswith('hd'):
                    still_name = split[0][0:-3]
                    if still_name == self.vid_name:
                        self._set_stills_paths(thing, os.path.join(dirpath, thing))
                        break

                elif split[0].lower().endswith('raw'):
                    still_name = split[0][0:-4]
                    if still_name == self.vid_name:
                        self._set_stills_paths(thing, os.path.join(dirpath, thing))
                        break
            else:
                logger.warning('did not find stills for {}'.format(self.vid_name))

    def _set_stills_paths(self, still, still_path):
        '''
        used by _get_stills_paths
        '''
        self.paths['poster'] = still_path
        self.paths['thumbnail'] = still_path
        logger.info('found {}'.format(still))

    def _get_music_info(self, music_dict):
        '''
        find music track, author, and source url from list of spreadsheet records
        '''
        vid_name = self.vid_name.replace('_', ' ').lower()

        for key in music_dict.keys():
            if key.lower().strip() == vid_name:
                self.music_track = music_dict[key]['music_track']
                self.music_track_author = music_dict[key]['music_track_author']
                self.urls['music_track'] = music_dict[key]['source_url']
                logger.info('found music info for {}'.format(self.name))
                break
        else:
            logger.warning('did not find music info for {}'.format(self.vid_name))

    def _get_source_ids(self, source_id_dict):
        '''
        find source ids from list of spreadsheet records
        '''
        vid_name = self.vid_name.replace('_', ' ').lower()

        for key in source_id_dict.keys():
            if key.lower().strip() == vid_name:
                self.source_id = source_id_dict[key]
                logger.info('found source ID for {}'.format(self.name))
                break
        else:
            logger.warning('did not find source ID for {}'.format(self.vid_name))

    def move(self):
        if self.paths['uploaded']:
            shutil.move(self.paths['source'], self.paths['uploaded'])
            logger.info('{} moved to local uploaded directory'.format(self.filename))


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
        logger.info('getting folder list...')

        url = 'https://cms.api.brightcove.com/v1/accounts/{}/folders'.format(self.pub_id)
        r = requests.get(url, headers=self._get_authorization_headers())

        if r.status_code == 200:
            for folder in r.json():
                folders[folder['name'].lower()] = folder['id']

            return folders
        else:
            logger.error('unable to get folder list')
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)
            sys.exit('exiting script')

    def search_for_video(self, ref_id):
        '''
        CMS API call to search for existing video by reference id
        '''
        logger.info('searching for reference_id {} on brightcove...'.format(ref_id))

        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/ref:{refid}".format(pubid=self.pub_id, refid=ref_id)
        r = requests.get(url, headers=self._get_authorization_headers())

        if r.status_code == 200:
            found_vid = r.json()
            logger.info('reference id {} exists as "{}"'.format(ref_id, found_vid['name']))
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
                'musictrackurl': video.urls['music_track'],
                'filename': video.filename,
            },
            'link': {
                'url': video.urls['recipe'],
                'text': '',
            }
        }

        json_data = json.dumps(data)
        r = requests.post(url, headers=self._get_authorization_headers(), data=json_data)

        if r.status_code == 201:
            vid_deets = r.json()
            video.json = vid_deets
            video.id = vid_deets['id']
            logger.info('created video object "{}" on brightcove'.format(video.name))
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
            logger.info('{} deleted from brightcove'.format(video.name))
        else:
            logger.error('unable to delete {}'.format(video.name))
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)

    def get_upload_urls(self, file, key):
        '''
        performs an authenticated request to discover a brightcove-provided
        location to securely upload a source file
        '''
        # obtain a file upload location
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/upload-urls/{sourcefilename}".format(pubid=self.pub_id, videoid=video.id, sourcefilename=key)
        r = requests.get(url, headers=self._get_authorization_headers())
        upload_urls_response = r.json()

        video.urls['upload'][key] = upload_urls_response['signed_url']
        video.urls['ingest'][key] = upload_urls_response['api_request_url']

        if r.status_code == 200:
            logger.info('received upload url for {}'.format(key))
        else:
            logger.error('did not receive upload url for {}'.format(key))
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)

    def upload(self, video, key):
        '''
        upload file to url provided by get_upload_url()
        '''
        # Upload the contents of our local file to the location provided via HTTP PUT
        # This is not recommended for large files
        with open(video.paths[key], 'rb') as fh:
            logger.info('uploading...')
            s = requests.put(video.urls['upload'][key], data=fh.read())

        if s.status_code == 200:
            logger.info('{} uploaded for {}'.format(key, video.name))
            # if video was uploaded successfully, set upload path, which acts as a flag for video.move()
            if key == 'source':
                video.paths['uploaded'] = os.path.join(uploaded_path, video.filename)
        else:
            logger.error('unable to upload {} for {}'.format(key, video.name))
            logger.error('status code: {}, reason: {}'.format(s.status_code, s.reason))
            logger.error(s.text)

    def di_request(self, video):
        '''
        Ingest API call to populate a video with transcoded renditions
        from a remotely accessible source asset
        '''
        url = "https://ingest.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/ingest-requests".format(pubid=self.pub_id, videoid=video.id)

        data = {
            'master': {
                'url': '{}'.format(video.urls['ingest']['video'])
            },
            'profile': 'videocloud-default-v1',
        }

        for word in ['poster', 'thumbnail']:
            if video.urls['ingest'][word]:
                data[word] = {
                    'url': video.urls['ingest'][word]
                }
                data['capture-images'] = False

        json_data = json.dumps(data)
        r = requests.post(url, headers=self._get_authorization_headers(), data=json_data)

        if r.status_code == 200:
            logger.info('files for {} ingested'.format(video.name))
        else:
            logger.error('unable to ingest files for {}'.format(video.name))
            logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
            logger.error(r.text)


class Spreadsheet(object):
    '''methods for interacting with Google Drive spreadsheets'''

    music_tracks_key = bright_brick_road.music_tracks_key
    master_list_key = bright_brick_road.master_list_key

    def __init__(self):
        self.gc = self._authenticate()
        self.sheets = self._get_sheets()
        self.music_dict = self._compile_music()
        self.source_dict = self._compile_sources()

    def _authenticate(self):
        '''
        authenticate with Google drive.
        first check for credentials in the office, then at home;
        return None if neither found
        '''
        logger.info('authenticating to Google Sheets...')
        scope = ['https://spreadsheets.google.com/feeds']

        if os.path.isfile(bright_brick_road.spread_cred):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(bright_brick_road.spread_cred, scope)

        elif os.path.isfile(bright_brick_road.spread_cred_home):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(bright_brick_road.spread_cred_home, scope)
            logger.info('working from home today i see =]')

        else:
            logger.error('unable to authenticate with gspread')
            sys.exit('exiting the script')

        return gspread.authorize(credentials)

    def _get_sheets(self):
        '''
        get spreadsheet objects from authenticated google drive instance
        '''
        logger.info('getting spreadsheets...')
        music = self.gc.open_by_key(Spreadsheet.music_tracks_key)
        master = self.gc.open_by_key(Spreadsheet.master_list_key)

        sheet_music = music.worksheet("Music Tracks")
        master_list_pending = master.worksheet('Localization Pending')
        master_list_completed = master.worksheet('Localization Completed')
        master_list_completed_US = master.worksheet('Localization Completed - US Videos')

        sheets = {
            'music': sheet_music,
            'pending': master_list_pending,
            'completed': master_list_completed,
            'completed_US': master_list_completed_US
        }

        return sheets

    def _compile_music(self):
        logger.info('compiling music list...')
        music_list = self.sheets['music'].get_all_records()

        return {music_list[i]['VIDEO']: {'music_track': music_list[i]['Music Title'], 'music_track_author': music_list[i]['Musician/Composer'], 'source_url': music_list[i]['Link to the Music Website']} for i in range(len(music_list))}

    def _compile_sources(self):
        logger.info('compiling source_ID list...')
        records_list = [self.sheets[key].get_all_records() for key in ['pending', 'completed', 'completed_US']]

        return {records_list[i][j]['VIDEO']: records_list[i][j]['SOURCE ID'] for i in range(len(records_list)) for j in range(len(records_list[i]))}


if __name__ == '__main__':
    logging.config.fileConfig('log.conf')
    logger = logging.getLogger('log')
    logger.info('* * * * * * * * * * * * * * * * * * * * \n')

    if not os.path.isdir(bright_brick_road.stills_base_path):
        logger.error('not connected to P Drive')
        sys.exit('not connected to P Drive')

    brightcove = Brightcove()
    spreadsheets = Spreadsheet()

    for dirpath, dirnames, filenames in os.walk(search_path):
        for filename in filenames:
            video = Video(filename, spreadsheets.music_dict, spreadsheets.source_dict)

            # search for a video on brightcove with same reference id
            search = brightcove.search_for_video(video.reference_id)

            if not search:
                # if no video found, create new video object and move it into the corresponding country folder
                brightcove.create_video(video)
                brightcove.move_to_folder(video)

            elif search:
                # else if video found, let us know we'll be replacing it
                logger.info('{} exists, replacing source file...'.format(video.filename))

            # get upload urls for video file (and stills if they exist), then upload
            for key in video.paths.keys():
                if video.paths[key]:
                    brightcove.get_upload_urls(video, key)
                    brightcove.upload(video, key)

            # call Dynamic Ingest API to ingest video, with stills as poster and thumbnail if applicable
            brightcove.di_request(video)
            video.move()
            logger.info(' ')
