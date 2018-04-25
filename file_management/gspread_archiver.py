#!/usr/local/bin/python3
# gspread archiver
# 5/19/16
# updated 12/21/17

import os
import shutil
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from windy_paths import spread_cred


localized_sets = [{'X'}, {'S,X'}, {'S', 'S,X'}, {'X', 'S,X'}, {'X', 'N/A'}, {'S', 'N/A'}, {'S,X', 'N/A'}, {'S,X', 'N/A,X'}, {'X', 'S,X', 'N/A'}, {'X', 'S,X', 'N/A'}]

source_paths = {
    "US_videos": "/Volumes/Video HD Raid 5/AR US Videos ",
    "localization": "/Volumes/Video HD Raid 5/Allrecipes International Video Projects/editingLocalizing",
}

archive_paths = {
    'archive': '/Volumes/Video HD Raid 5/Dropbox (Meredith)/ARCHIVE/archived',
    'archive_US': '/Volumes/Video HD Raid 5/Dropbox (Meredith)/ARCHIVE/archived_US',
    'move': '/Volumes/Video_Localized/Localized',
    'move_US': '/Volumes/Video_Localized/US_videos',
}


def authenticate():
    '''
    authenticates account with Google drive
    returns a tuple of the Pending tab and the archived by robot spreadsheet from the Master List
    '''
    print('authenticating to Google Sheets...')

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(spread_cred, scope)

    gc = gspread.authorize(credentials)

    sheet = gc.open("Allrecipes Master Video List")
    master_list_pending = sheet.worksheet('Localization Pending')
    archived_by_robot = sheet.worksheet('archived by robot')

    return master_list_pending, archived_by_robot


def replace_chars(string):
    name = string

    for x in [',', ':']:
        name = name.replace(x, '')

    for y in ['-', ' ']:
        name = name.replace(y, '_')

    return name


def get_localized(spreadsheet):
    localized_videos = {}

    for x in range(spreadsheet.row_count):
        row_value = spreadsheet.row_values(x + 1)
        # only look at UK, AU, QC, and US columns in spreadsheet
        row_value_set = set([row_value[i] for i in [3, 5, 13, 15]])
        if row_value_set in localized_sets:
            name = replace_chars(row_value[0])
            localized_videos[name] = x + 1

    if any(localized_videos):
        print("\nVIDEOS READY FOR ARCHIVING IN MASTER LIST:")

        localized_list = list(localized_videos.keys())
        localized_list.sort()
        for video in localized_list:
            print("{}".format(video))
    else:
        print("\nNO VIDEOS FOR ARCHIVING FOUND IN MASTER LIST")

    return localized_videos


def find_videos(paths, archive_paths, filename_dict):
    '''
    finds videos to archive on the computer based on videos found in the Master List
    takes in filename_dict and creates video_dict with following format:
    vid_name: src_path, archive_path, path_to_move_dir_to_on_Video_Localized_drive
    '''
    video_dict = {}

    for key in source_paths:
        for dir in os.scandir(source_paths[key]):
            if dir.name in filename_dict:
                if key == "US_videos":
                    video_dict[dir.name] = dir.path, os.path.join(archive_paths['archive_US'], dir.name), os.path.join(archive_paths['move_US'], dir.name), filename_dict[dir.name]
                else:
                    video_dict[dir.name] = dir.path, os.path.join(archive_paths['archive'], dir.name), os.path.join(archive_paths['move'], dir.name), filename_dict[dir.name]

    if any(video_dict):
        print("\nFOUND THESE VIDEOS TO ARCHIVE ON THE COMPUTER:")
        localized_list_02 = list(video_dict.keys())
        localized_list_02.sort()

        for video in localized_list_02:
            print("{}".format(video))

        if filename_dict.keys() == video_dict.keys():
            print("\nALL VIDEOS FROM MASTER LIST FOUND ON COMPUTER")
        else:
            print("\nTHESE VIDEOS WERE NOT FOUND ON THE COMPUTER:")
            missing = set(filename_dict.keys()) - set(video_dict.keys())

            for video in missing:
                print("{}".format(video))

    else:
        print("\nDIDN'T FIND ANY VIDEOS TO ARCHIVE ON COMPUTER")

    return video_dict


def archive(video_dict, spreadsheets):
    for key in video_dict:
        src_file = video_dict[key][0]
        archive_file = video_dict[key][1]
        move_file = video_dict[key][2]
        row_num = video_dict[key][3]
        existing_archive = (archive_file + '.zip')

        if os.path.exists(existing_archive):
            print("\n{} archive already exists, removing...".format(key))
            os.remove(existing_archive)

        print("\narchiving {}...".format(key))
        shutil.make_archive(archive_file, 'zip', src_file)

        if os.path.exists(move_file):
            print("{} already exists at destination, did not move".format(key))
            moved = False
        else:
            print("moving {}...".format(key))
            shutil.move(src_file, move_file)
            moved = True

        update_sheet(row_num, moved, spreadsheets)
        print("done")


def update_sheet(row_num, moved, spreadsheets):
    values = spreadsheets[0].row_values(row_num)

    if not moved:
        values.append('*** not moved')

    print("updating spreadsheet...")
    spreadsheets[1].append_row(values)


if __name__ == '__main__':
    spreadsheets = authenticate()
    video_dict = find_videos(source_paths, archive_paths, get_localized(spreadsheets[0]))
    archive(video_dict, spreadsheets)
