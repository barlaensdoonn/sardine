#!/usr/local/bin/python3
# get created dates for each video on vimeo from api
# 2/4/17

import csv
import datetime
import vimeo
import vimeo_credents

# api request goes here, while loop will step through pages until exhausted
api_call = '/me/videos?sort=alphabetical'


def call_api(caller, call):
    next_page = call
    vid_list = []

    while next_page:
        videos = caller.get(next_page)
        data = videos.json()['data']

        for i in range(len(data)):
            created = datetime.datetime.strptime(data[i]['created_time'][0:10], '%Y-%m-%d')
            vid_date = '{:02}/{:02}/{}'.format(created.month, created.day, created.year)
            month_year = '{:02}/{}'.format(created.month, created.year)

            tupled = (data[i]['name'], vid_date, month_year)
            vid_list.append(tupled)

        # will return None on last page
        next_page = videos.json()['paging']['next']

    return vid_list


def write_results(vid_list):
    with open(vimeo_credents.dates_output_file, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['VIDEO', 'DATE CREATED', 'MONTH/YEAR CREATED'])
        for row in vid_list:
            csv_out.writerow(row)


if __name__ == '__main__':
    vimeod = vimeo.VimeoClient(token=vimeo_credents.access_token, key=vimeo_credents.client_identifier, secret=vimeo_credents.client_secrets)

    listed = call_api(vimeod, api_call)
    write_results(listed)
