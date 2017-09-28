#!/usr/local/bin/python3
# get info for all videos in youtube channel
# 7/17/17
# updated 7/17/17

import secret
import pickle
import httplib2
import traceback
from datetime import datetime, timedelta

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow


# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the {{ Google Cloud Console }} at {{ https://cloud.google.com/console }}.
# Please ensure that you have enabled the YouTube Data and YouTube Analytics APIs for your project.
# For more information about using OAuth2 to access the YouTube Data API, see: https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets.json file format, see: https://developers.google.com/api-client-library/python/guide/aaa_client_secrets

# These OAuth 2.0 access scopes allow for read-only access to the authenticated
# user's account for both YouTube Data API resources and YouTube Analytics Data.
youtube_scopes = ["https://www.googleapis.com/auth/youtube.readonly",
                  "https://www.googleapis.com/auth/yt-analytics.readonly",
                  # "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
                  ]

countries = ["AU", "AR", "BR", "DE", "FR", "IT", "MX", "NL", "PL", "QC", "RU", "UK"]  # used to loop through the client_secrets_files
client_secrets_files = secret.client_secrets_files


class AuthenticatedQueries(object):
    '''
    authenticate to youtube data api and youtube analytics api with oauth2,
    get and return an analytics query response
    '''

    youtube_api_service_name = "youtube"
    youtube_api_version = "v3"
    youtube_analytics_api_service_name = "youtubeAnalytics"
    youtube_analytics_api_version = "v1"

    def __init__(self, secrets_files, scopes):
        self.secrets_files = secrets_files
        self.scopes = scopes

    def parse_cli_arguments(self):
        now = datetime.now()
        one_day_ago = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        # one_week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        alltime = "2011-01-01"

        # other callable metrics: estimatedMinutesWatched,averageViewDuration,averageViewPercentage,estimatedRevenue,cardClickRate
        argparser.add_argument("--metrics", default="views,comments,likes,dislikes,shares,subscribersGained,subscribersLost", help="Report metrics")
        argparser.add_argument("--start-date", default=alltime, help="Start date, in YYYY-MM-DD format")
        argparser.add_argument("--end-date", default=one_day_ago, help="End date, in YYYY-MM-DD format")
        argparser.add_argument("--alt", default="json", help="format for report, either 'json' or 'csv'")
        argparser.add_argument("--sort", default="-views", help="Sort order")

        self.args = argparser.parse_args()

    def get_authenticated_services(self, oauth_file_path):
        # This variable defines a message to display if the CLIENT_SECRETS_FILE is missing.
        missing_clients_secrets_message = """
        WARNING: Please configure OAuth 2.0

        To make this run you will need to populate the client_secrets.json file
        found at:

           {}

        with information from the {{ Cloud Console }}
        {{ https://cloud.google.com/console }}

        For more information about the client_secrets.json file format, please visit:
        https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
        """.format(client_secrets_files[oauth_file_path])

        flow = flow_from_clientsecrets(client_secrets_files[oauth_file_path],
                                       scope=" ".join(youtube_scopes),
                                       message=missing_clients_secrets_message)

        # TODO: make this more generic by not using script name to make file name
        storage = Storage("/Users/kestrel/gitBucket/credentials/oaths/{}-{}-oauth2.json".format("analytics_test_multiple_channel.py", oauth_file_path))
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            credentials = run_flow(flow, storage, self.args)

        http = credentials.authorize(httplib2.Http())

        self.youtube = build(self.youtube_api_service_name, self.youtube_api_version, http=http)
        self.youtube_analytics = build(self.youtube_analytics_api_service_name, self.youtube_analytics_api_version, http=http)

    def get_channel_id(self):
        self.channels_list_response = self.youtube.channels().list(mine=True, part="id").execute()

        self.channel_id = self.channels_list_response["items"][0]["id"]


def get_now():
    now = datetime.now()
    return now.strftime('%m_%d_%y')


def pickle_data(data, now_str):
    pckl_path = 'misc/YT_pickles/all_info/YT_info_all_channels_{}.p'.format(now_str)
    print('got info for all channels, pickling as: {}'.format(pckl_path))

    with open(pckl_path, 'wb') as pickl:
        pickle.dump(data, pickl, protocol=pickle.HIGHEST_PROTOCOL)


def main():
    authenticated_queries = AuthenticatedQueries(client_secrets_files, youtube_scopes)
    authenticated_queries.parse_cli_arguments()
    vid_dict = {country: [] for country in countries}

    for country in countries:
        try:
            print('getting all info for {}'.format(country))
            authenticated_queries.get_authenticated_services(country)

            # taken from https://github.com/youtube/api-samples/blob/master/python/my_uploads.py
            channels_response = authenticated_queries.youtube.channels().list(mine=True, part="contentDetails").execute()
            upload_playlist_id = channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            upload_playlist_request = authenticated_queries.youtube.playlistItems().list(playlistId=upload_playlist_id, part='snippet', maxResults=50)

            while upload_playlist_request:
                r = upload_playlist_request.execute()
                print('got 50 videos...')
                for item in r['items']:
                    # NOTE: to search this later, use item['snippet']['title']
                    vid_dict[country].append(item)

                upload_playlist_request = authenticated_queries.youtube.playlistItems().list_next(upload_playlist_request, r)

            print('got channel info for {}\n'.format(country))
        except Exception:
            print('\nexception encountered trying to get channel info for {}\n'.format(country))
            traceback.print_exc()

    pickle_data(vid_dict, get_now())


if __name__ == "__main__":
    main()
