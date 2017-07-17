#!/usr/local/bin/python3
# compare videos on Brightcove to videos on YouTube by country
# 6/28/17
# updated 7/17/17

# import os
# import sys
import csv
import pickle
import itertools
from datetime import datetime
# import get_vids_by_folder
#
# sys.path.append(os.path.abspath(os.path.join('..', 'youtube_analytics')))
# import get_vids_by_channel


countries = ["AU", "AR", "BR", "DE", "FR", "IT", "MX", "NL", "PL", "QC", "RU", "UK"]
output_country = input('which country would you like output?\n')


if __name__ == '__main__':
    now = datetime.now()
    today = now.strftime('%m_%d_%y')

    with open('ignore/BC_vids_by_folder_{}.p'.format(today), 'rb') as pckl:
        bcv = pickle.load(pckl)

    with open('../youtube_analytics/misc/YT_vids_by_channel_{}.p'.format(today), 'rb') as pckl:
        ytb = pickle.load(pckl)

    print('\n-----{}-----'.format(output_country))
    print('ytb: {}'.format(len(ytb[output_country])))
    print('bcv: {}'.format(len(bcv[output_country.lower()])))
    print('diff: {}\n'.format(len(ytb[output_country]) - len(bcv[output_country.lower()])))

    ytb[output_country].sort()
    bcv[output_country.lower()].sort()

    filename = 'ignore/YT_BC_comparison_{}_{}.csv'.format(output_country, today)
    zipped = itertools.zip_longest(ytb[output_country], bcv[output_country.lower()])

    with open(filename, 'w') as out:
        writer = csv.writer(out)
        writer.writerow(('YouTube', 'Brightcove'))
        writer.writerows(zipped)

    print('successfully output {}'.format(filename))
