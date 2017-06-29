#!/usr/local/bin/python3
# combined file copier
# 5/16/16
# updated: 6/28/17

import gspread
import os
import shutil
import traceback
from oauth2client.service_account import ServiceAccountCredentials
import windy_paths


# do not change order of flags without updating make_sheets methods in spreadsheet class
flags = ['copy', 'copy_zip', 'copy_zip_US', 'not_found', 'stills']


class Spreadsheet(object):

    def __init__(self):
        self.country_columns = {
            "AR": 5, "AU": 6, "BR": 7, "DE": 8, "FR": 9, "IT": 10, "MX": 11, "NL": 12, "PL": 13, "QC": 14, "RU": 15, "UK": 4
        }

    def _replace_chars(self, string):
        name = string

        for x in [',', ':', "'", '?', '(', ')']:
            name = name.replace(x, '')

        for y in ['-', ' ']:
            name = name.replace(y, '_')

        return name

    def _get_sheet_names(self, spreadsheet):
        sheet_names = {}
        video_names = spreadsheet.col_values(1)

        for item in video_names:
            if item != '':
                if not item.isupper():
                    name = self._replace_chars(item).lower()
                    sheet_names[name] = item

        return sheet_names

    def authenticate(self):
        '''
        authenticates account with Google drive
        sets class variable for relevant spreadsheet tabs in the Master List
        '''
        print('authenticating to Google Sheets...\n')

        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(windy_paths.spread_cred, scope)

        gc = gspread.authorize(credentials)

        sheet = gc.open("Allrecipes Master Video List")
        master_list_pending = sheet.worksheet('Localization Pending')
        master_list_completed = sheet.worksheet('Localization Completed')
        master_list_completed_US = sheet.worksheet('Localization Completed - US Videos')

        self.spreadsheets = (master_list_pending, master_list_completed, master_list_completed_US)

    def make_sheet_names_dict(self):
        self.sheet_names_dict = {flags[i]: self._get_sheet_names(self.spreadsheets[i]) for i in range(len(self.spreadsheets))}

    def make_sheets_dict(self):
        self.sheets_dict = {flags[i]: self.spreadsheets[i] for i in range(len(self.spreadsheets))}

    def update_sheet(self, spreadsheet, sheet_names, vid_name, country):
        found_vid = spreadsheet.find(sheet_names[vid_name])
        spreadsheet.update_cell(found_vid.row, self.country_columns[country], 'X')


class Copier(object):

    def __init__(self):
        self.src_dir = windy_paths.copier_src_dir
        self.export_path = 'Exports/localizedVP9'
        self.stills_path = 'Stills'
        self.duplicates = {}
        self.countries = ["AR", "AU", "BR", "DE", "FR", "IT", "MX", "NL", "PL", "QC", "RU", "UK"]
        self.stats = {flags[i]: [] for i in range(len(flags))}
        self.dropbox_paths = {country: os.path.join('/Volumes/Video HD Raid 5/Dropbox (Meredith)/by_country/{}'.format(country)) for country in self.countries}

        self.stills_paths = {
            'square': '/Volumes/public/International/Editorial/Video/Recipe Images/SQUARE',
            'hd': '/Volumes/public/International/Editorial/Video/Recipe Images/YouTube 1280x720',
            'raw': '/Volumes/public/International/Editorial/Video/Recipe Images/Raw Images',
        }

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
            "FR": os.path.join(windy_paths.base_path, 'Google Drive/Videos for review'),
            "IT": "/Volumes/public/International/Editorial/Video/Videos by country/IT/_New videos",
            "MX": "/Volumes/public/International/Editorial/Video/Videos by country/MX/_New videos",
            "NL": "/Volumes/public/International/Editorial/Video/Videos by country/NL/_New videos",
            "QC": "/Volumes/public/International/Editorial/Video/Videos by country/QC/_New videos",
            "PL": "/Volumes/Video HD Raid 5/Dropbox (Meredith)/PL",
            "RU": "/Volumes/public/International/Editorial/Video/Videos by country/RU/_New videos",
            "UK": "/Volumes/Video HD Raid 5/Dropbox (Meredith)/UK",
        }

    def _copy_stills(self, vid_name, archive_path):
        '''
        stills_base_path is the video's Stills path, i.e.:
        /Volumes/Video HD Raid 5/Allrecipes International Video Projects/editingLocalizing/Perfect_gluten_free_sponge_cake/Stills
        '''
        print('checking for stills on P Drive...')
        stills_base_path = os.path.join("/".join(archive_path.split("/")[:-3]), self.stills_path)

        for pic in os.scandir(stills_base_path):
            filename = os.path.splitext(pic.name)[0].lower()
            file_end = filename.split('_')[-1]

            if file_end in self.stills_paths.keys():
                still_dst = os.path.join(self.stills_paths[file_end], pic.name)
            elif file_end == '250' or file_end == '960':
                still_dst = os.path.join(self.stills_paths['square'], pic.name)
            else:
                still_dst = None

            if still_dst and not os.path.isfile(still_dst):
                print('copying {}'.format(pic.name))
                shutil.copy2(pic.path, still_dst)
                self.stats['stills'].append(pic.name)

            elif still_dst and os.path.isfile(still_dst):
                print('{} already exists'.format(pic.name))

        print('\n')

    def _copy_dropbox(self, country, file_name, src_file):
        drop = os.path.join(self.dropbox_paths[country], file_name)

        if os.path.exists(drop):
            print("removing old {} from dropbox country folder...".format(file_name))
            os.remove(drop)
        print("copying new {} to dropbox country folder...".format(file_name))
        shutil.copy2(src_file, drop)

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
        makes a dict as {'file': ('vid_name', 'COUNTRY', social, 'src_file')}
        where file is complete file name, such as 'Hot_cross_buns_UK.webm'
        vid name is name of the video (which should be its directory too), such as 'Hot_cross_buns'
        COUNTRY is 2 letter country for that file, such as 'UK'
        social is a boolean flag for social videos
        src_file is full path to source file to use for copying

        if more than one video from same directory in filenames, add the number
        of duplicates to self.duplicates dict as {'vid_name': # of times duplicated}
        '''
        vid_names = []
        vid_countries = []
        socials = []

        for vid in self.filenames:
            strip = os.path.splitext(vid)
            split = strip[0].split('_')

            country = split[-1]

            if split[-2].lower() == 'social':
                social = True
                vid_name = '_'.join(split[:-2])
            else:
                social = False
                vid_name = '_'.join(split[:-1])

            vid_names.append(vid_name)
            vid_countries.append(country)
            socials.append(social)

        # vid_names = [file[:(len(file) - 8)] for file in self.filenames]
        # vid_countries = [file[(len(file) - 7):(len(file) - 5)] for file in self.filenames]
        vid_split = list(zip(vid_names, vid_countries, socials, self.src_files))

        for vid in vid_names:
            if vid_names.count(vid) > 1 and vid not in self.duplicates:
                self.duplicates[vid] = vid_names.count(vid) - 1

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
            for thing in os.scandir(self.archive_paths[key]):
                if thing.name == vid_name:
                    if not os.path.exists(os.path.join(thing.path, self.export_path)):
                        print("{} improperly set up for {}".format(self.export_path, vid_name))
                        print("creating {} directory".format(os.path.join(vid_name, self.export_path)))
                        os.mkdir(os.path.join(thing.path, self.export_path))

                    if key == 'video_localized':
                        flag = "copy_zip"
                        backup_src = thing.path

                    elif key == 'video_localized_US':
                        flag = "copy_zip_US"
                        backup_src = thing.path

                    else:
                        flag = "copy"
                        backup_src = None

                    dst_file = os.path.join(thing.path, self.export_path, file_name)

                    return dst_file, backup_src, flag

        else:
            print("didn't find {} anywhere".format(file_name))
            flag = "not_found"
            backup_src = None
            dst_file = os.path.join(windy_paths.no_copy_dir, file_name)

            return dst_file, backup_src, flag

    def find_country_path(self, country, file_name):
        if country in self.countries:
            dst_file = os.path.join(self.country_paths[country], file_name)

            return dst_file

    def copy(self, vid_name, country, social, file_name, src_file, archive_path, backup_src, flag, country_path):

        if flag[0:4] == 'copy':
            if country == 'UK':
                self._copy_stills(vid_name, archive_path)

            if os.path.exists(archive_path):
                print("removing old {} from local archive...".format(file_name))
                os.remove(archive_path)
            print("copying new {} to local archive...".format(file_name))
            shutil.copy2(src_file, archive_path)

            if os.path.exists(country_path):
                print("removing old {} from country folder...".format(file_name))
                os.remove(country_path)
            print("copying new {} to country folder...".format(file_name))
            shutil.copy2(src_file, country_path)

            self._copy_dropbox(country, file_name, src_file)

            if social:
                print("moving {} to uploaded folder\n".format(file_name))
                shutil.move(src_file, os.path.join(windy_paths.uploaded_dir, file_name))
            else:
                print("moving {} to brightcove folder\n".format(file_name))
                shutil.move(src_file, os.path.join(windy_paths.brightcove_dir, file_name))

        else:
            print("moving {} to no_copy folder\n".format(file_name))
            shutil.move(src_file, windy_paths.no_copy_dir)

        if backup_src:
            backup_dst = os.path.join(self.zip_paths[flag], backup_src.split("/")[-1])
            backup_dst_zip = backup_dst + '.zip'

            if os.path.isfile(backup_dst_zip):
                print('{}.zip already exists in DropBox, removing...'.format(vid_name))
                os.remove(backup_dst_zip)
            print('archiving {} to DropBox...\n'.format(vid_name))
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
    if not os.path.isdir(copier.country_paths['FR']):
        raise Exception("not connected to Google Drive")
    if not os.path.isdir(copier.country_paths['PL']):
        raise Exception("DropBox directory not configured")

    sheets.authenticate()
    sheets.make_sheet_names_dict()
    sheets.make_sheets_dict()

    copier.clean_up()
    copier.make_vid_dict()

    for key in copier.vid_dict:
        file_name = key
        vid_name = copier.vid_dict[key][0]
        country = copier.vid_dict[key][1]
        social = copier.vid_dict[key][2]
        src_file = copier.vid_dict[key][3]
        archive_path, backup_src, flag = copier.find_archive_path(vid_name, file_name)
        country_path = copier.find_country_path(country, file_name)

        # if two videos from same directory are being copied and video is in archive
        # don't create archive until last video has been copied (set backup_src to None)
        if vid_name in copier.duplicates and backup_src:
            if copier.duplicates[vid_name]:
                backup_src = None
                copier.duplicates[vid_name] -= 1

        copier.copy(vid_name, country, social, file_name, src_file, archive_path, backup_src, flag, country_path)

        if flag not in ['copy_zip', 'copy_zip_US', 'not_found'] and not social:
            try:
                sheets.update_sheet(sheets.sheets_dict[flag], sheets.sheet_names_dict[flag], vid_name.lower(), country)
            except Exception as e:
                print('could not update spreadsheet for {}:'.format(vid_name))
                traceback.print_exc()
                print("\n")

    copier.print_report()
