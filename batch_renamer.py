#!/usr/local/bin/python3
# archive video into DropBox country folder

# TODO: sort files into folders by codec


import os
import csv
import traceback
from windy_paths import filename_map_path, modified_dirs_path


class Renamer(object):

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

    def rename_file(self, file, new_filename, path):
        old_file = os.path.join(dirpath, file)
        new_file = os.path.join(dirpath, new_filename)
        os.rename(old_file, new_file)

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
        print('total file size (GB): {:,}'.format(self.file_size_counter / 1000000000))
        print('total file size (GB [1024]): {:,}'.format(self.file_size_counter / 1048576000))
        print('total # of files: {:,}\n'.format(self.count))

        print('------ total files by country ------')
        for key in sorted(self.country_count.keys()):
            print('{}: {}'.format(key.upper(), self.country_count[key]))
        print("\n")


if __name__ == '__main__':
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

                                renamer.add_to_count(*package)
                                renamer.new_file = renamer.get_new_file_name(*package[0:2])

                                if renamer.new_file != file:
                                    holder = (file, renamer.new_file, dirpath)
                                    renamer.append_to_record(holder)
                                    renamer.rename_file(*holder)

        renamer.print_records_to_file()
        renamer.print_counts()

    except Exception as e:
        print("did not finish, printing what's been done so far to files...\n")
        renamer.print_records_to_file()
        print("counts up to the point of the error:")
        renamer.print_counts()
        print(traceback.format_exc())
