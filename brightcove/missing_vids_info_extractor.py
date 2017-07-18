#!/usr/local/bin/python3
# use pickled youtube data to extract info for videos missing on Brightcove
# 7/17/17
# updated 7/17/17

import pickle
from datetime import datetime


now = datetime.now()
today = now.strftime('%m_%d_%y')
country = 'UK'
missing_vids = []


def replace(title):
    bad_strs = ['-', '|', 'Allrecipes.co.uk', 'video']
    title = title.strip()
    split = title.split(' ')

    for string in split:
        if string in bad_strs:
            split.remove(string)

    return ' '.join(split).strip()


with open('ignore/missing_vids_{}_{}_alternate.txt'.format(country, today), 'r') as missings:
    for vid in missings:
        missing_vids.append(vid.strip())


with open('../youtube_analytics/misc/YT_info_all_channels_{}.p'.format(today), 'rb') as pckl:
    ytb = pickle.load(pckl)


for i in range(len(ytb[country])):
    info = ytb[country][i]
    title = info['snippet']['title']
    for vid in missing_vids:
        if title.lower() in vid.lower():
            print(title.upper())
            print('URL:')
            print('https://www.youtube.com/watch?v={}\n'.format(info['snippet']['resourceId']['videoId']))
            print('{}\n'.format(info['snippet']['description']))
            print(' * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * \n')
