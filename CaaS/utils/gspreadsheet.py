#!/usr/local/bin/python3
# gspread functions
# 7/31/17
# updated 5/25/18

import os
import sys
import yaml
import gspread
import logging
import logging.config
import caas_keys
from oauth2client.service_account import ServiceAccountCredentials


class Gsheet(object):
    '''methods for interacting with Google Drive spreadsheets'''

    log_file = 'gspreadsheet.log'
    credents = [caas_keys.spread_cred, caas_keys.spread_cred_home, caas_keys.spread_cred_laptop]

    def __init__(self, logger=None):
        self.logger = logger if logger else self._init_logger()
        self.gc = self._authenticate()

    def _exit(self):
        self.logger.error('exiting...')
        sys.exit()

    def _init_logger(self):
        with open('log.yaml', 'r') as log_conf:
            log_config = yaml.safe_load(log_conf)

        log_config['handlers']['file']['filename'] = self.log_file
        logging.config.dictConfig(log_config)
        logging.info('* * * * * * * * * * * * * * * * * * * *')
        logging.info('logging configured in gspreadsheet.py')

        return logging.getLogger('gsheet')

    def _authenticate(self):
        '''
        authenticate with Google drive.
        first check for credentials in the office, then at home;
        return None if neither found
        '''
        self.logger.info('authenticating to Google Sheets...')
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        for credent in self.credents:
            if os.path.isfile(credent):
                credentials = ServiceAccountCredentials.from_json_keyfile_name(credent, scope)
                return gspread.authorize(credentials)
        else:
            self.logger.error("didn't find any valid gspread credential files")
            self._exit()

    def get_spreadsheet(self, key=None, url=None, title=None):
        '''try to find spreadsheet from a key, url, or title. all arguments should be strings'''

        methods = {
            key: self.gc.open_by_key,
            url: self.gc.open_by_url,
            title: self.gc.open
        }

        for method in methods.keys():
            if method:
                try:
                    # call the gspread method that corresponds to the supplied
                    # spreadsheet identifier and pass in the identifier
                    sht = methods[method](method)
                    self.logger.info('found spreadsheet id {}'.format(method))
                    return sht
                except gspread.SpreadsheetNotFound:
                    self.logger.warning('did not find spreadsheet id {}'.format(method))

    def get_worksheet(self, gsheet, title):
        '''return a specific tab in the spreadsheet as a gspread worksheet'''
        return gsheet.worksheet(title)
