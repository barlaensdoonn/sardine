#!/usr/local/bin/python3
# 1/30/17


import os
import shutil
import csv
import logging
import traceback
from windy_paths import filename_map_path, modified_dirs_path


class Renamer(object):
    '''
    if leading part of filename does not match the master video path,
    rename the file while preserving its country extension.
    copy to directory by country regardless of rename

    TODO: sort files into folders by codec
    '''

    def __init__(self):
        self.search_path = "/Volumes/Video_Localized"
        self.countries = ('ar', 'au', 'br', 'de', 'fr', 'it', 'mx', 'nl', 'pl', 'qc', 'ru', 'uk')
        self.country_paths = {country: os.path.join('/Volumes/Video_Localized/by_country', country.upper()) for country in self.countries}
        self.country_count = {self.countries[i]: 0 for i in range(len(self.countries))}
        self.extensions = ('.webm', '.mov', '.mp4', '.m4v')
        self.filename_map = []
        self.dir_list = []
        self.count = 0
        self.file_size_counter = 0
        self.rename_count = 0
        self.copy_count = 0

    def split_path(self, path):
        return [item.lower().strip() for item in path.split('/')]

    def add_to_count(self, country, dirpath, file):
        self.count += 1
        self.file_size_counter += os.stat(os.path.join(dirpath, file)).st_size
        self.country_count[country] += 1

    def get_new_file_name(self, country, dirpath):
        self.vid_name = dirpath.split('/')[4]
        return self.vid_name + '_' + country.upper() + self.split[1]

    def append_to_record(self, holder):
        self.filename_map.append(holder)
        if (self.vid_name,) not in self.dir_list:
            self.dir_list.append((self.vid_name,))

    def rename_file(self, old_file, path):
        old_file_path = os.path.join(path, old_file)
        self.new_file_path = os.path.join(path, renamer.new_file)
        os.rename(old_file_path, self.new_file_path)
        logging.info('renamed {} to {}'.format(old_file_path, self.new_file_path))
        self.rename_count += 1

    def country_copy(self, country, dirpath):
        country_file = os.path.join(self.country_paths[country], self.new_file)
        shutil.copy2(self.new_file_path, country_file)
        logging.info('copied {} to {}'.format(self.new_file_path, country_file))
        self.copy_count += 1

    def print_records_to_file(self):
        with open(filename_map_path, 'w') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(['OLD_FILE', 'NEW_FILE', 'PARENT PATH'])
            for row in self.filename_map:
                csv_out.writerow(row)

        with open(modified_dirs_path, 'w') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(['MODIFIED_DIRECTORIES'])
            for row in self.dir_list:
                csv_out.writerow(row)

    def print_counts(self):
        total_file_size = self.file_size_counter / 1000000000
        total_file_size_1024 = self.file_size_counter / 1048576000
        print('total file size (GB): {:,}'.format(total_file_size))
        print('total file size (GB [1024]): {:,}'.format(total_file_size_1024))
        print('total # of files: {:,}\n'.format(self.count))
        print('total # of files renamed: {}'.format(self.rename_count))
        print('total # of files copied: {}'.format(self.copy_count))
        logging.info('total file size (GB): {:,}'.format(total_file_size))
        logging.info('total file size (GB [1024]): {:,}'.format(total_file_size_1024))
        logging.info('total # of files: {:,}\n'.format(self.count))
        logging.info('total # of files renamed: {}'.format(self.rename_count))
        logging.info('total # of files copied: {}'.format(self.copy_count))

        print('------ total files by country ------')
        logging.info('------ total files by country ------')
        for key in sorted(self.country_count.keys()):
            print('{}: {}'.format(key.upper(), self.country_count[key]))
            logging.info('{}: {}'.format(key.upper(), self.country_count[key]))
        print("\n")


if __name__ == '__main__':
    logging.basicConfig(filename='/Volumes/Video_Localized/rename_log.log', format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S', level=logging.INFO)
    renamer = Renamer()

    try:
        for dirpath, dirnames, filenames in os.walk(renamer.search_path):
            path_split = renamer.split_path(dirpath)

            # 2nd half of conditional excludes a single directory in How_to_decorate_Easter_cupcakes
            if 'exports' in path_split and 'singles' not in path_split:
                for file in filenames:
                    renamer.split = os.path.splitext(file)
                    if renamer.split[1].lower() in renamer.extensions:
                        for country in renamer.countries:
                            if renamer.split[0].lower().endswith(country):
                                package = (country, dirpath, file)

                                # add video to global count regardless of rename
                                renamer.add_to_count(*package)
                                renamer.new_file = renamer.get_new_file_name(*package[0:2])

                                # rename file if not formatted properly
                                if renamer.new_file != file:
                                    holder = (file, renamer.new_file, dirpath)
                                    renamer.append_to_record(holder)
                                    renamer.rename_file(file, dirpath)

                                # after rename, copy to country specific folder
                                renamer.country_copy(country, dirpath)

        renamer.print_records_to_file()
        renamer.print_counts()

    except Exception as e:
        print("did not finish, printing what's been done so far to files...\n")
        renamer.print_records_to_file()
        print("counts up to the point of the error:")
        renamer.print_counts()

        print(traceback.format_exc())
        logging.error(traceback.format_exc())
