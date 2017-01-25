#!/usr/local/bin/python3
# archive video into DropBox country folder

# TODO: if file already exists, add to another list and don't do anything
# TODO: sort files into folders by codec
# TODO: same renaming for premiere pro projects

import os
import csv
from windy_paths import filename_map_path, modified_dirs_path

search_path = "/Volumes/Video_Localized"
countries = ('ar', 'au', 'br', 'de', 'fr', 'it', 'mx', 'nl', 'pl', 'qc', 'ru', 'uk')
country_count = {countries[i]: 0 for i in range(len(countries))}
extensions = ('.webm', '.mov', '.mp4', '.m4v')
filename_map = []
dir_list = []
count = 0
counter = 0


for dirpath, dirnames, filenames in os.walk(search_path):
    path_split = [item.lower().strip() for item in dirpath.split('/')]

    # 2nd half of conditional excludes single directory in How_to_decorate_Easter_cupcakes
    if 'exports' in path_split and 'singles' not in path_split:
        for file in filenames:
            split = os.path.splitext(file)
            if split[1].lower() in extensions:
                for country in countries:
                    if split[0].lower().endswith(country):
                        count += 1
                        counter += os.stat(os.path.join(dirpath, file)).st_size
                        country_count[country] += 1

                        vid_name = dirpath.split('/')[4]
                        new_file = vid_name + '_' + country.upper() + split[1]
                        if new_file != file:
                            holder = (file, new_file, dirpath)
                            filename_map.append(holder)
                            if (vid_name,) not in dir_list:
                                dir_list.append((vid_name,))


with open(filename_map_path, 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(['OLD_FILE', 'NEW_FILE', 'PARENT PATH'])
    for row in filename_map:
        csv_out.writerow(row)

with open(modified_dirs_path, 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(['MODIFIED_DIRECTORIES'])
    for row in dir_list:
        csv_out.writerow(row)

print('total file size (GB): {:,}'.format(counter / 1000000000))
print('total file size (GB [1024]): {:,}'.format(counter / 1048576000))
print('total # of files: {:,}\n'.format(count))

print('------ total files by country ------')
for key in sorted(country_count.keys()):
    print('{}: {}'.format(key, country_count[key]))
