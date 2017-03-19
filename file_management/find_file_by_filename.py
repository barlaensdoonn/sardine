#!/usr/local/bin/python3

# find files by their filename ending
# 1/31/17

import os

'''
code to search directory (including nested dirs) for filename that ends with a string
for example code below will print out all files that end with string in find tuple regardless of extension
i.e. 'How_to_make_cupcakes_copy.webm'
NOTE: always beware the empty space, like: 'copy .mp4'
NOTE: strings in find should be lowercase
'''

search_path = '/Volumes/Video_Localized'
find = ('copy', ' ', '1')


for dirpath, dirnames, filenames in os.walk(search_path):
    # these 2 lines: only search in directories that include 'exports' in the path
    # path_split = [item.lower().strip() for item in dirpath.split('/')]
    # if 'exports' in path_split:

    for file in filenames:
        file_split = os.path.splitext(file)
        for thing in find:
            if file_split[0].lower().endswith(thing):
                file_path = os.path.join(dirpath, file)
                print(file_path)
