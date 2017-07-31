#!/usr/local/bin/python3
# brightcove from spreadsheet
# 4/18/17
# updated 7/31/17

import os
import sys
import json
import shutil
import pandas
import requests
import logging
import logging.config
import gspreadsheet
import bright_brick_road
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


search_path = bright_brick_road.search_path
uploaded_path = bright_brick_road.uploaded
csv_path = '/Volumes/Video_Localized/logs/missing_vids_QC.csv'

try:
    proceed = input('\nwould you like to use the following csv?\n{}\n'.format(csv_path))
    if proceed.lower() not in ['yes', 'y']:
        csv_path = input('please paste in the full path to the csv:\n')
except KeyboardInterrupt:
    sys.exit('...user exit received...')


class Video(object):
    '''holds video info for input into Brightcove class'''

    stills_path = bright_brick_road.stills_base_path

    def __init__(self, direntry, sprdsht, music_dict, source_id_dict):
        '''
        self.paths['uploaded'] acts as a flag:
        if the video was uploaded successfully, the path is made in brightcove.upload(),
        and will evaluate to True in video.move(); if upload was unsuccessful,
        it stays as None and evaluates to False, and video is not moved
        '''
        self.filename = direntry.name
        self.name = os.path.splitext(self.filename)[0]
        self.vid_name = self.name[0:-3]
        self.sheet_name = self.vid_name.replace('_', ' ').lower()
        self.country = self.name[-2:].lower()
        self.state = 'INACTIVE'
        self.id = None
        self.json = None

        self.paths = {
            'video': direntry.path,
            'poster': None,
            'thumbnail': None,
            'uploaded': None
        }

        self.urls = {
            'music_track': None,
            'youtube': None,
            'recipe': None,
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

        self._get_info(sprdsht)
        self._get_music_info(music_dict)
        self._get_source_ids(source_id_dict)
        self._get_stills_paths()

    def _exit(self):
        logger.warning('exiting...')
        sys.exit()

    def _get_info(self, sprdsht):
        try:
            info = sprdsht.loc[self.sheet_name]
        except Exception:
            logger.warning('did not find info for {} in spreadsheet'.format(self.vid_name))
            logger.warning('exiting...')
            sys.exit()

        self.title = info['Localized title']
        # self.description = info['Description']
        self.reference_id = info['RecipeID-Country']

        self.urls['youtube'] = info['YT URL']
        self.urls['recipe'] = info['Recipe URL']

        if info['YT Tags'] != 'None':
            self.tags = info['YT Tags'].split(', ')
            self.tags.append(self.country)
        else:
            self.tags = [self.country]

        logger.info('retrieved info for {} from spreadsheet'.format(self.vid_name))

    def _get_music_info(self, music_dict):
        '''
        find music track, author, and source url from list of spreadsheet records
        '''
        vid_name = self.vid_name.replace('_', ' ').lower()

        for key in music_dict.keys():
            # handle dashes in video titles
            title = key.replace('-', ' ')

            if title.lower().strip() == vid_name:
                self.music_track = music_dict[key]['music_track']
                self.music_track_author = music_dict[key]['music_track_author']
                self.urls['music_track'] = music_dict[key]['source_url']
                logger.info('found music info for {}'.format(self.name))
                break
        else:
            logger.warning('did not find music info for {}'.format(self.vid_name))
            self._exit()

    def _get_source_ids(self, source_id_dict):
        '''
        find source ids from list of spreadsheet records
        '''
        vid_name = self.vid_name.replace('_', ' ').lower()

        for key in source_id_dict.keys():
            # handle dashes in video titles
            title = key.replace('-', ' ')

            if title.lower().strip() == vid_name:
                self.source_id = source_id_dict[key]
                logger.info('found source ID for {}'.format(self.name))
                break
        else:
            logger.warning('did not find source ID for {}'.format(self.vid_name))
            self._exit()

    def _set_stills_paths(self, still, still_path):
        '''
        used by _get_stills_paths
        '''
        self.paths['poster'] = still_path
        self.paths['thumbnail'] = still_path
        logger.info('found {}'.format(still))

    def _get_stills_paths(self):
        '''
        utility function to find stills to use for DI API poster and thumbnail images
        '''
        still_path = Video.stills_path

        # look for HD images first
        for dirpath, dirnames, filenames in os.walk(still_path):
            for thing in filenames:
                split = os.path.splitext(thing)[0]

                if 'hd' in split.lower():
                    if split.lower().startswith(self.vid_name.lower()):
                        self._set_stills_paths(thing, os.path.join(dirpath, thing))
                        return

        # look for RAW images next (these aren't always the correct aspect ratio)
        for dirpath, dirnames, filenames in os.walk(still_path):
            for thing in filenames:
                split = os.path.splitext(thing)[0]

                if 'raw' in split.lower():
                    if split.lower().startswith(self.vid_name.lower()):
                        self._set_stills_paths(thing, os.path.join(dirpath, thing))
                        return
        else:
            logger.warning('did not find stills for {}'.format(self.vid_name))
            logger.warning('exiting...')
            sys.exit()

    def move(self):
        if self.paths['uploaded']:
            shutil.move(self.paths['video'], self.paths['uploaded'])
            logger.info('{} moved to local uploaded directory'.format(self.filename))


class Brightcove(object):
    '''methods for interacting with Brightcove CMS and DI APIs'''

    def __init__(self):
        logger.info('initializing brightcove class...')
        self.pub_id = bright_brick_road.pub_id
        self.folders = self._get_folders()

    def _exit(self):
        logger.error('exiting...')
        sys.exit()

    def _log_error_and_exit(self, r):
        logger.error('status code: {}, reason: {}'.format(r.status_code, r.reason))
        logger.error(r.text)
        self._exit()

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
            self._log_error_and_exit(r)

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
            self._log_error_and_exit(r)

    def search_for_video(self, ref_id):
        '''
        CMS API call to search for existing video by reference id
        '''
        if ref_id:
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

        else:
            return None

    def create_video(self, video):
        '''
        CMS API call to create a video in the VideoCloud catalog
        '''
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/".format(pubid=self.pub_id)
        data = {
            'name': video.title,
            # 'long_description': video.description,
            'reference_id': video.reference_id,
            'state': video.state,
            'tags': video.tags,
            'custom_fields': {
                'sourceid': video.source_id,
                'musictrack': video.music_track,
                'musictrackauthor': video.music_track_author,
                'musictrackurl': video.urls['music_track'],
                'filename': video.filename,
                'ytvideourl': video.urls['youtube']
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
            logger.info('created video object {} on brightcove'.format(video.title))
        else:
            logger.error('unable to create video object {}'.format(video.name))
            self._log_error_and_exit(r)

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
            self._log_error_and_exit(r)

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
            self._log_error_and_exit(r)

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
            self._log_error_and_exit(r)

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
            if key == 'video':
                video.paths['uploaded'] = os.path.join(uploaded_path, video.filename)
        else:
            logger.error('unable to upload {} for {}'.format(key, video.name))
            self._log_error_and_exit(s)

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
            self._log_error_and_exit(r)


if __name__ == '__main__':
    logging.config.fileConfig('log.conf')
    logger = logging.getLogger('log')
    logger.info('* * * * * * * * * * * * * * * * * * * *')

    if not os.path.isdir(bright_brick_road.stills_base_path):
        logger.error('not connected to P Drive')
        sys.exit('not connected to P Drive')

    brightcove = Brightcove()
    spreadsheets = gspreadsheet.Spreadsheet(logger)

    # make dataframe from spreadsheet columns using lowercase video names as index
    vids = pandas.read_csv(csv_path, index_col=0)
    vids.index = vids.index.str.lower()

    for direntry in os.scandir(search_path):

            # skip this common OSX hidden file
            if direntry.name != '.DS_Store':
                video = Video(direntry, vids, spreadsheets.music_dict, spreadsheets.source_dict)

                # search for a video on brightcove with same reference id
                search = brightcove.search_for_video(video.reference_id)

                if not search:
                    # if no video found, create new video object and move it into the corresponding country folder
                    brightcove.create_video(video)
                    brightcove.move_to_folder(video)

                elif search:
                    # else if video found, let us know we'll be replacing it
                    logger.info('replacing source file with {}...'.format(video.filename))

                # get upload urls for video file (and stills if they exist), then upload
                for key in video.paths.keys():
                    if video.paths[key] and key != 'uploaded':
                        brightcove.get_upload_urls(video, key)
                        brightcove.upload(video, key)

                # call Dynamic Ingest API to ingest video, with stills as poster and thumbnail if applicable
                brightcove.di_request(video)
                video.move()
                logger.info(' - - - - - - - - - - - - - - - - - - - ')

    logger.info('* * * * * * * * * * * * * * * * * * * *\n')
