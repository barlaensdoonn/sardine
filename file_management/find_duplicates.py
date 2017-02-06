#!/usr/local/bin/python3

# find duplicate files
# 2/1/17

import os

'''
code to search directory (including nested dirs) for duplicate files by filename before file extension
for example code below will print out both these files:
'How_to_make_cupcakes_UK.webm' & 'How_to_make_cupcakes_UK.mov'
'''

search_path = '/Volumes/Video_Localized/by_country'
vid_list = []
dup_list = []


for dirpath, dirnames, filenames in os.walk(search_path):
    for file in filenames:
        # weed out common OSX specific file
        if file != '.DS_Store':
            file_split = os.path.splitext(file)
            vid_list.append(file_split[0])

dup_list = [thing for thing in vid_list if vid_list.count(thing) >= 2]
dup_set = set(dup_list)

with open('./duplies.txt', 'w') as output:
    for dirpath, dirnames, filenames in os.walk(search_path):
        for file in filenames:
            file_split = os.path.splitext(file)
            if file_split[0] in dup_set:
                output.write('{}{}'.format(os.path.join(dirpath, file), '\n'))
