#!/usr/local/bin/python3
# combined file copier
# 5/16/16

import gspread
import os
import shutil
from oauth2client.service_account import ServiceAccountCredentials


flags = ['copy', 'copy_zip', 'copy_zip_US', 'not_found']


class Spreadsheet(object):

    def __init__(self):
        self.country_columns = {
            "AR": 5, "AU": 6, "BR": 7, "DE": 8, "FR": 9, "IT": 10, "MX": 11, "NL": 12, "PL": 13, "QC": 14, "RU": 15, "UK": 4
        }

    def authenticate(self):
        '''
        authenticates account with Google drive
        sets class variable for relevant spreadsheet tabs in the Master List
        '''
        print('authenticating to Google Sheets...\n')

        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/baleson/Documents/credentials/googleAuth/spreadsheet-access-5092af4cc743.json', scope)

        gc = gspread.authorize(credentials)

        sheet = gc.open("Allrecipes Master Video List")
        master_list_pending = sheet.worksheet('Localization Pending')
        master_list_completed = sheet.worksheet('Localization Completed')
        master_list_completed_US = sheet.worksheet('Localization Completed - US Videos')

        self.spreadsheets = (master_list_pending, master_list_completed, master_list_completed_US)

    def replace_chars(self, string):
        name = string

        for x in [',', ':']:
            name = name.replace(x, '')

        for y in ['-', ' ']:
            name = name.replace(y, '_')

        return name

    def get_sheet_names(self, spreadsheet):
        sheet_names = {}
        video_names = spreadsheet.col_values(1)

        for item in video_names:
            if item != '':
                if not item.isupper():
                    name = self.replace_chars(item).lower()
                    sheet_names[name] = item

        return sheet_names

    def make_sheet_names_dict(self):
        self.sheet_names_dict = {flags[i]: self.get_sheet_names(self.spreadsheets[i]) for i in range(len(self.spreadsheets))}

    def make_sheets_dict(self):
        self.sheets_dict = {flags[i]: self.spreadsheets[i] for i in range(len(self.spreadsheets))}

    def update_sheet(self, spreadsheet, sheet_names, vid_name, country):
        found_vid = spreadsheet.find(sheet_names[vid_name])
        spreadsheet.update_cell(found_vid.row, self.country_columns[country], 'X')


class Copier(object):

    def __init__(self):
        self.src_dir = '/Users/baleson/Desktop/python_squeeze_me/exportsForLocalization'
        self.extra_path = 'Exports/localizedVP9'
        self.no_copy_dir = '/Users/baleson/Desktop/python_squeeze_me/no_copy'
        self.copied_dir = '/Users/baleson/Desktop/python_squeeze_me/copied'
        self.countries = ["AR", "AU", "BR", "DE", "FR", "IT", "MX", "NL", "PL", "QC", "RU", "UK"]

        self.archive_paths = {
            "video_raid": '/Volumes/Video HD Raid 5/Allrecipes International Video Projects/editingLocalizing',
            "video_raid_US": '/Volumes/Video HD Raid 5/AR US Videos ',
            "video_localized": '/Volumes/Video_Localized/Localized',
            "video_localized_US": '/Volumes/Video_Localized/US_videos',
        }

        self.zip_paths = {
            'copy_zip': '/Volumes/Video HD Raid 5/Dropbox (Meredith)/ARCHIVE/archived',
            'copy_zip_US': '/Volumes/Video HD Raid 5/Dropbox (Meredith)/ARCHIVE/archived_US',
        }

        self.country_paths = {
            "AR": "/Volumes/public/International/Editorial/Video/Videos by country/AR/_New videos",
            "AU": "/Volumes/public/International/Editorial/Video/Videos by country/AU/_New videos",
            "BR": "/Volumes/public/International/Editorial/Video/Videos by country/BR/_New videos",
            "DE": "/Volumes/public/International/Editorial/Video/Videos by country/DE/_New videos",
            "FR": "/Users/baleson/Google Drive/Videos for review",
            "IT": "/Volumes/public/International/Editorial/Video/Videos by country/IT/_New videos",
            "MX": "/Volumes/public/International/Editorial/Video/Videos by country/MX/_New videos",
            "NL": "/Volumes/public/International/Editorial/Video/Videos by country/NL/_New videos",
            "QC": "/Volumes/public/International/Editorial/Video/Videos by country/QC/_New videos",
            "PL": "/Volumes/Video HD Raid 5/Dropbox (Meredith)/PL",
            "RU": "/Volumes/public/International/Editorial/Video/Videos by country/RU/_New videos",
            "UK": "/Volumes/Video HD Raid 5/Dropbox (Meredith)/UK",
        }

        self.stats = {flags[i]: [] for i in range(len(flags))}

    def clean_up(self):
        '''
        cleans up the filenames list by removing .xmp files and .DS_Store.
        also deletes .xmp files from directory
        returns a tuple of the cleaned up filenames list, and
        the src_files list of the full paths that point to the source files for copying
        '''
        for dirpath, dirnames, filenames in os.walk(self.src_dir):
            for file in filenames:

                if file == '.DS_Store':
                    filenames.remove(file)

                if file[(len(file) - 4):len(file)] == ".xmp":
                    print("found a stray .xmp file, deleting: {}".format(file))
                    os.remove(os.path.join(dirpath, file))
                    filenames.remove(file)

            self.src_files = [os.path.join(dirpath, file) for file in filenames]
            self.filenames = filenames

    def make_vid_dict(self):
        '''
        makes a dict as {'file': ('vid_name', 'COUNTRY', 'src_file')}
        where file is complete file name, such as 'Hot_cross_buns_UK.webm'
        vid name is name of the video (which should be its directory too), such as 'Hot_cross_buns'
        COUNTRY is 2 letter country for that file, such as 'UK'
        src_file is full path to source file to use for copying
        '''
        vid_names = [file[:(len(file) - 8)] for file in self.filenames]
        vid_countries = [file[(len(file) - 7):(len(file) - 5)] for file in self.filenames]
        vid_split = list(zip(vid_names, vid_countries, self.src_files))

        self.vid_dict = {file: splits for (file, splits) in zip(self.filenames, vid_split)}

    def find_archive_path(self, vid_name, file_name):
        '''
        scans through {archive_paths} to look for video directory, returns a tuple based on the results.

        if no Exports/localizedVP9 directory exists, one is made
        if it finds /vid_name/Exports/localized, returns dst_file with the full dir plus "copy" skip flag
        - - if above is found on Video_Localized then flag is "copy_zip" or "copy_zip_US" depending on directory - -
        if it doesn't find /vid_name anywhere, returns dst_file with no_copy_dir plus "not_found" skip flag
        '''
        for key in self.archive_paths:
            for dir in os.scandir(self.archive_paths[key]):
                if dir.name == vid_name:
                    if not os.path.exists(os.path.join(dir.path, self.extra_path)):
                        print("{} improperly set up for {}".format(self.extra_path, vid_name))
                        print("creating {} directory".format(os.path.join(vid_name, self.extra_path)))
                        os.mkdir(os.path.join(dir.path, self.extra_path))

                    if key == 'video_localized':
                        flag = "copy_zip"
                        backup_src = dir.path

                    elif key == 'video_localized_US':
                        flag = "copy_zip_US"
                        backup_src = dir.path

                    else:
                        flag = "copy"
                        backup_src = None

                    dst_file = os.path.join(dir.path, self.extra_path, file_name)

                    return dst_file, backup_src, flag

        else:
            print("didn't find {} anywhere".format(file_name))
            flag = "not_found"
            backup_src = None
            dst_file = os.path.join(self.no_copy_dir, file_name)

            return dst_file, backup_src, flag

    def find_country_path(self, country, file_name):
        if country in self.countries:
            dst_file = os.path.join(self.country_paths[country], file_name)

            return dst_file

    def copy(self, file_name, src_file, archive_path, backup_src, flag, country_path):

        # TODO: if same video is copied for multiple countries, do not archive video each time, wait until after last country is copied

        if flag[0:4] == 'copy':
            if os.path.exists(archive_path):
                print("removing old {} from archive...".format(file_name))
                os.remove(archive_path)
            print("copying {} to archive...\n".format(file_name))
            shutil.copy2(src_file, archive_path)

            if os.path.exists(country_path):
                print("removing old {} from country folder...".format(file_name))
                os.remove(country_path)
            print("copying {} to country folder...\n".format(file_name))
            shutil.copy2(src_file, country_path)
            shutil.move(src_file, os.path.join(self.copied_dir, file_name))

        else:
            print("moving {} no_copy folder\n".format(file_name, "/".join(archive_path.split("/")[2:])))
            shutil.move(src_file, archive_path)

        if backup_src:
            backup_dst = os.path.join(self.zip_paths[flag], backup_src.split("/")[-1])
            backup_dst_zip = backup_dst + '.zip'

            if os.path.isfile(backup_dst_zip):
                print('{} already exists, removing...'.format(backup_dst_zip))
                os.remove(backup_dst_zip)
            print('archiving {}\n'.format(backup_dst))
            shutil.make_archive(backup_dst, 'zip', backup_src)

        self.stats[flag].append(file_name)

    def print_report(self):
        print('{:*^50}'.format('SUMMARY'))

        if all(x == [] for x in self.stats.values()):
            print("nothing to do, i'm bored")

        else:
            for key in self.stats:
                if any(self.stats[key]):
                    self.stats[key].sort()
                    print('{:-^40}'.format(key.upper()))
                    for item in self.stats[key]:
                        print(item)


if __name__ == '__main__':

    sheets = Spreadsheet()
    copier = Copier()

    if not os.path.isdir(copier.country_paths['AR']):
        raise Exception("not connected to P Drive")

    sheets.authenticate()
    sheets.make_sheet_names_dict()
    sheets.make_sheets_dict()

    copier.clean_up()
    copier.make_vid_dict()

    for key in copier.vid_dict:
        file_name = key
        vid_name = copier.vid_dict[key][0]
        country = copier.vid_dict[key][1]
        src_file = copier.vid_dict[key][2]
        archive_path, backup_src, flag = copier.find_archive_path(vid_name, file_name)
        country_path = copier.find_country_path(country, file_name)
        copier.copy(file_name, src_file, archive_path, backup_src, flag, country_path)

        if flag != 'not_found':
            sheets.update_sheet(sheets.sheets_dict[flag], sheets.sheet_names_dict[flag], vid_name.lower(), country)

    copier.print_report()
