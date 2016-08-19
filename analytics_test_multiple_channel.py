#!/Library/Frameworks/Python.framework/Versions/3.5/bin/python3

from datetime import datetime, timedelta
import httplib2
import sys
import plotly.plotly as py
import plotly.graph_objs as graphs

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
CLIENT_SECRETS_FILES = {
    "AU": "./credentials/client_ids/client_id_AU.json",
    "AR": "./credentials/client_ids/client_id_AU.json",
    "BR": "./credentials/client_ids/client_id_BR.json",
    "DE": "./credentials/client_ids/client_id_DE.json",
    "FR": "./credentials/client_ids/client_id_FR.json",
    "IT": "./credentials/client_ids/client_id_IT.json",
    "MX": "./credentials/client_ids/client_id_MX.json",
    "NL": "./credentials/client_ids/client_id_NL.json",
    "QC": "./credentials/client_ids/client_id_NL.json",
    "RU": "./credentials/client_ids/client_id_RU.json",
    "UK": "./credentials/client_ids/client_id_UK.json"
}

# list used to loop through the CLIENT_SECRETS_FILES to authenticate and get analytics
COUNTRIES = ["AU", "AR", "BR", "DE", "FR", "IT", "MX", "NL", "QC", "RU", "UK"]

views_dict = {}

# these will hold labels and corresponding values for pie chart
labels = []
values = []

# These OAuth 2.0 access scopes allow for read-only access to the authenticated
# user's account for both YouTube Data API resources and YouTube Analytics Data.
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly",
                  "https://www.googleapis.com/auth/yt-analytics.readonly",
                  # "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
                  ]

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_ANALYTICS_API_SERVICE_NAME = "youtubeAnalytics"
YOUTUBE_ANALYTICS_API_VERSION = "v1"


def get_authenticated_services(args, oauth_file_path):
    # This variable defines a message to display if the CLIENT_SECRETS_FILE is missing.
    MISSING_CLIENT_SECRETS_MESSAGE = """
    WARNING: Please configure OAuth 2.0

    To make this run you will need to populate the client_secrets.json file
    found at:

       {}

    with information from the {{ Cloud Console }}
    {{ https://cloud.google.com/console }}

    For more information about the client_secrets.json file format, please visit:
    https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
    """.format(CLIENT_SECRETS_FILES[oauth_file_path])

    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILES[oauth_file_path],
                                   scope=" ".join(YOUTUBE_SCOPES),
                                   message=MISSING_CLIENT_SECRETS_MESSAGE)

    storage = Storage("./credentials/oaths/{}-{}-oauth2.json".format(sys.argv[0], oauth_file_path))
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage, args)

    http = credentials.authorize(httplib2.Http())

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=http)
    youtube_analytics = build(YOUTUBE_ANALYTICS_API_SERVICE_NAME, YOUTUBE_ANALYTICS_API_VERSION, http=http)

    return (youtube, youtube_analytics)


def get_channel_id(youtube):
    channels_list_response = youtube.channels().list(mine=True, part="id").execute()

    return channels_list_response["items"][0]["id"]


def print_report(analytics_query_response):
    print("Analytics Data for {} Channel".format(channel_id[1]))

    for column_header in analytics_query_response.get("columnHeaders", []):
        print("{:<20}".format(column_header["name"]), end='')
    print("")

    for row in analytics_query_response.get("rows", []):
        for value in row:
            print("{:<20.0f}".format(value), end='')
    print("\n")


def run_analytics_report(youtube_analytics, channel_id, options):
    # Call the Analytics API to retrieve a report. For a list of available reports, see: https://developers.google.com/youtube/analytics/v1/channel_reports
    analytics_query_response = youtube_analytics.reports().query(
        ids="channel=={}".format(channel_id[0]),
        metrics=options.metrics,
        # dimensions=options.dimensions,
        start_date=options.start_date,
        end_date=options.end_date,
        alt=options.alt,
        # max_results=options.max_results,
        sort=options.sort
    ).execute()

    print_report(analytics_query_response)

    return analytics_query_response


if __name__ == "__main__":
    now = datetime.now()
    one_day_ago = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    one_week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    alltime = "2011-01-01"

    # added these lines
    # argparser.add_argument("--channel-id", help="channel id", default="channel_id")
    # argparser.add_argument("content-owner-id", help="content owner id")

    # other callable metrics: estimatedMinutesWatched,averageViewDuration,averageViewPercentage,estimatedRevenue,cardClickRate
    argparser.add_argument("--metrics", default="views,comments,likes,dislikes,shares,subscribersGained,subscribersLost", help="Report metrics")
    # argparser.add_argument("--dimensions", help="Report dimensions", default="video")
    argparser.add_argument("--start-date", default=alltime, help="Start date, in YYYY-MM-DD format")
    argparser.add_argument("--end-date", default=one_day_ago, help="End date, in YYYY-MM-DD format")
    argparser.add_argument("--alt", default="json", help="format for report, either 'json' or 'csv'")
    # argparser.add_argument("--max-results", help="Max results", default=10)
    argparser.add_argument("--sort", default="-views", help="Sort order")
    args = argparser.parse_args()

    for country in COUNTRIES:
        (youtube, youtube_analytics) = get_authenticated_services(args, country)
        try:
            channel_id = (get_channel_id(youtube), country)
            report = run_analytics_report(youtube_analytics, channel_id, args)
            views_dict[country] = int(report['rows'][0][0])
        except HttpError as e:
            print("An HTTP error {} occurred:".format(e.resp.status))
            print("{}".format(e.content))

    for key, value in views_dict.items():
        labels.append(key)
        values.append(value)

    views_dict['total'] = sum(value for value in views_dict.values())
    for key, value in views_dict.items():
        print('{}: {}'.format(key, value))
