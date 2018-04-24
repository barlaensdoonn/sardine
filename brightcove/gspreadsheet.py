#!/usr/local/bin/python3
# gspread functions
# 7/31/17
# updated 7/31/17

import os
import sys
import gspread
import logging
import logging.config
import bright_brick_road
from oauth2client.service_account import ServiceAccountCredentials


class Spreadsheet(object):
    '''methods for interacting with Google Drive spreadsheets'''

    music_tracks_key = bright_brick_road.music_tracks_key
    master_list_key = bright_brick_road.master_list_key

    def __init__(self, logger):
        self.logger = logger
        self.gc = self._authenticate()
        self.sheets = self._get_sheets()
        self.music_dict = self._compile_music()
        self.source_dict = self._compile_sources()

    def _exit(self):
        self.logger.error('exiting...')
        sys.exit()

    def _authenticate(self):
        '''
        authenticate with Google drive.
        first check for credentials in the office, then at home;
        return None if neither found
        '''
        self.logger.info('authenticating to Google Sheets...')
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        if os.path.isfile(bright_brick_road.spread_cred):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(bright_brick_road.spread_cred, scope)

        elif os.path.isfile(bright_brick_road.spread_cred_home):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(bright_brick_road.spread_cred_home, scope)
            self.logger.info('working from home today i see =]')

        else:
            self.logger.error('unable to authenticate with gspread')
            self._exit()

        return gspread.authorize(credentials)

    def _get_sheets(self):
        '''
        get spreadsheet objects from authenticated google drive instance
        '''
        self.logger.info('getting spreadsheets...')
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
        self.logger.info('compiling music list...')
        music_list = self.sheets['music'].get_all_records()

        return {music_list[i]['VIDEO']: {'music_track': music_list[i]['Music Title'], 'music_track_author': music_list[i]['Musician/Composer'], 'source_url': music_list[i]['Link to the Music Website']} for i in range(len(music_list))}

    def _compile_sources(self):
        self.logger.info('compiling source_ID list...')
        records_list = [self.sheets[key].get_all_records() for key in ['pending', 'completed', 'completed_US']]

        return {records_list[i][j]['VIDEO']: records_list[i][j]['SOURCE ID'] for i in range(len(records_list)) for j in range(len(records_list[i]))}

    def get_spreadsheet(self, title):
        '''
        try to find spreadsheet for video
        '''
        try:
            sht = self.gc.open(title)
            self.logger.info('found spreadsheet titled {}'.format(title))
            return sht
        except gspread.SpreadsheetNotFound:
            self.logger.warning('did not find spreadsheet titled {}'.format(title))
            self._exit()
