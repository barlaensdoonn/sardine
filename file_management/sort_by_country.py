#!/usr/local/bin/python3

# copy videos to directories sorted by country
# 3/19/17

import os
import shutil
import csv
import logging

'''
code to search directory (including nested dirs) for filename that ends
with a two letter country abbreviation, and copy file to that country's directory.
i.e. 'How_to_make_cupcakes_UK.webm' is copied to ''/Volumes/Video_Localized/by_country_again/UK/'
'''

log_file = '/Volumes/Video_Localized/by_country_again/sort_by_country.log'
search_path = '/Volumes/Video_Localized'
countries_path = '/Volumes/Video_Localized/by_country_again'
countries = ('ar', 'au', 'br', 'de', 'fr', 'it', 'mx', 'nl', 'pl', 'qc', 'ru', 'uk')


def copy_files():
    filename_map = []

    for dirpath, dirnames, filenames in os.walk(search_path):
        # these 2 lines: only search in directories that include 'exports' in the path
        path_split = [item.lower().strip() for item in dirpath.split('/')]
        if 'exports' in path_split:

            for file in filenames:
                file_split = os.path.splitext(file)

                for country in countries:
                    if file_split[0].lower().endswith(country):
                        file_path = os.path.join(dirpath, file)
                        copy_path = os.path.join(countries_path, country.upper(), file)
                        wrapper = (file_path, copy_path)

                        shutil.copy2(*wrapper)
                        logging.info('{}  copied to  {}'.format(*wrapper))
                        filename_map.append(wrapper)

                        print(file_path)
                        print('{}\n'.format(copy_path))

    return filename_map


def write_to_file(filename_map):
    filename_map_path = '/Volumes/Video_Localized/by_country_again/filename_map.csv'

    with open(filename_map_path, 'w') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(['SRC_FILE', 'COPIED_FILE'])
            for row in filename_map:
                csv_out.writerow(row)


if __name__ == '__main__':
    logging.basicConfig(filename=log_file, format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S', level=logging.INFO)

    record = copy_files()
    write_to_file(record)
