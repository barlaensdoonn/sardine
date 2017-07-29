#!/usr/local/bin/python3
# get plays and likes for each video on vimeo from api
# 2/5/17

import csv
import vimeo
import vimeo_credents


# api request goes here, while loop will step through pages until exhausted
api_call = '/me/videos?sort=alphabetical'


def call_api(caller, call):
    next_page = call
    vid_list = []
    plays = 0
    likes = 0
    total_duration = 0

    while next_page:
        videos = caller.get(next_page)
        data = videos.json()['data']

        for i in range(len(data)):
            play = data[i]['stats']['plays']
            like = data[i]['metadata']['connections']['likes']['total']
            duration = data[i]['duration']

            plays += play
            likes += like
            total_duration += duration

            tupled = (data[i]['name'], play, like, duration)
            vid_list.append(tupled)

        next_page = videos.json()['paging']['next']

    return (vid_list, plays, likes, total_duration)


def write_results(vid_list, plays, likes, total_duration):
    with open(vimeo_credents.stats_output_file, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['VIDEO', 'PLAYS', 'LIKES', 'DURATION [seconds]'])

        for row in vid_list:
            csv_out.writerow(row)

        csv_out.writerow([])
        csv_out.writerow(['TOTAL PLAYS', 'TOTAL LIKES', 'TOTAL DURATION [seconds]'])
        csv_out.writerow([plays, likes, total_duration])


if __name__ == '__main__':
    vimeod = vimeo.VimeoClient(token=vimeo_credents.stats_access_token, key=vimeo_credents.client_identifier, secret=vimeo_credents.client_secrets)

    listed = call_api(vimeod, api_call)
    write_results(*listed)

    print('TOTAL PLAYS: {:,}'.format(listed[1]))
    print('TOTAL LIKES: {:,}'.format(listed[2]))
    print('TOTAL DURATION: {:,}'.format(listed[3]))
