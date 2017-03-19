# suffix file copier
# 11/10/16

import os
import shutil

src_dir = "/Volumes/Video_Localized"
dst_dir = "/Volumes/public/International/Editorial/Video/Recipe Images/Raw Images"

count = 0

for dirpath, dirnames, filenames in os.walk(src_dir):
    for filename in filenames:
        if filename[len(filename) - 7:len(filename) - 4] == 'RAW':
            try:
                src_file = os.path.join(dirpath, filename)
                shutil.copy2(src_file, dst_dir)
                print("copied {}".format(filename))
                count += 1
            except PermissionError as e:
                print("permission conflict with {}, skipping...".format(filename))

print("copied {} files".format(count))
