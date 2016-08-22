#!/Library/Frameworks/Python.framework/Versions/3.5/bin/python3

from datetime import datetime, timedelta
import httplib2
import sys
import plotly.plotly as py
# import plotly.graph_objs as graphs

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
    "AU": "../credentials/client_ids/client_id_AU.json",
    "AR": "../credentials/client_ids/client_id_AU.json",
    "BR": "../credentials/client_ids/client_id_BR.json",
    "DE": "../credentials/client_ids/client_id_DE.json",
    "FR": "../credentials/client_ids/client_id_FR.json",
    "IT": "../credentials/client_ids/client_id_IT.json",
    "MX": "../credentials/client_ids/client_id_MX.json",
    "NL": "../credentials/client_ids/client_id_NL.json",
    "QC": "../credentials/client_ids/client_id_NL.json",
    "RU": "../credentials/client_ids/client_id_RU.json",
    "UK": "../credentials/client_ids/client_id_UK.json"
}

# list used to loop through the CLIENT_SECRETS_FILES to authenticate and get analytics
COUNTRIES = ["AU", "AR", "BR", "DE", "FR", "IT", "MX", "NL", "QC", "RU", "UK"]

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


def parse_cli_arguments():
    now = datetime.now()
    one_day_ago = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    # one_week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    alltime = "2011-01-01"

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

    return args


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

    storage = Storage("../credentials/oaths/{}-{}-oauth2.json".format(sys.argv[0], oauth_file_path))
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


def run_analytics_report(youtube_analytics, channel_id, options):
    '''
    Call the Analytics API to retrieve a report. For a list of available reports,
    see: https://developers.google.com/youtube/analytics/v1/channel_reports
    '''
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

    return analytics_query_response


def print_report(analytics_query_response):
    '''
    parses the analytics API query response - which can be JSON or CSV - and prints relevant info.
    '''
    print("ANALYTICS DATA FOR {}'s CHANNEL".format(channel_id[1]))

    for column_header in analytics_query_response.get("columnHeaders", []):
        print("{:<20}".format(column_header["name"]), end='')
    print("")

    for row in analytics_query_response.get("rows", []):
        for value in row:
            print("{:<20.0f}".format(value), end='')
    print("\n")


def compute_totals(metrics_dict):
    for key in metrics.keys():
        metrics[key]['total'] = sum(value for value in metrics[key].values())


def update_views_pie(metrics_dict):
    labels = []
    values = []

    for key, value in metrics_dict['views'].items():
        if key != 'total':
            labels.append(key)
            values.append(value)

    print('updating pie...\n')

    pie_get = py.get_figure("https://plot.ly/~allrecipes_international/2/")
    data = pie_get.data

    # to construct dict for computing view differences from last update:
    # past_views_dict = {label: value for label, value in zip(data[0]['labels'], data[0]['values'])}

    data.update({'values': values, 'labels': labels})

    py.plot(pie_get, filename='youtube channel views pie', auto_open=False)


def update_views_graph(metrics_dict):
    # this function relies on 'total' being a key in the metrics['views'] dict

    print("updating graph...\n")

    sorted_views = sort_metrics(metrics_dict, 'views')
    x = [sorted_views[i][0] for i in range(len(sorted_views)) if sorted_views[i][0] != 'total']
    y = [sorted_views[i][1] for i in range(len(sorted_views)) if sorted_views[i][0] != 'total']
    total_views = 'TOTAL VIEWS: ' + '{:,}'.format(metrics_dict['views']['total'])

    views_graph = py.get_figure("https://plot.ly/~allrecipes_international/4/")
    views_graph_annotations = views_graph.layout.annotations
    views_graph_data = views_graph.data

    views_graph_data.update({'x': x, 'y': y})
    views_graph_annotations.update({'text': total_views})

    py.plot(views_graph, filename='youtube channel views graph', auto_open=False)


def sort_metrics(metrics_dict, key):

    return sorted(metrics_dict[key].items(), key=lambda x: x[1], reverse=True)


def print_sorted_metrics(metrics_dict):
    for key in metrics_dict.keys():

        print(key.upper())

        sorted_metrics = sort_metrics(metrics_dict, key)
        for i in range(len(sorted_metrics)):
            print('{}: {:,}'.format(sorted_metrics[i][0], sorted_metrics[i][1]))

        print('\n')


if __name__ == "__main__":

    cli_args = parse_cli_arguments()

    # create a list of the metric names in the report from command line arguments passed to --metrics
    # the order of these doesn't matter because the metric names are keys in the metrics dict
    # that hold dicts with country-specific values
    columnHeaders = cli_args.metrics.split(',')
    metrics = {column: {} for column in columnHeaders}

    for country in COUNTRIES:

        (youtube, youtube_analytics) = get_authenticated_services(cli_args, country)

        try:
            channel_id = (get_channel_id(youtube), country)
            report = run_analytics_report(youtube_analytics, channel_id, cli_args)
            print_report(report)

            # columnHeaders = [report['columnHeaders'][i]['name'] for i in range(len(report['columnHeaders']))]

            # update dicts in metrics with country specific values corresponding to a report metric
            for i in range(len(columnHeaders)):
                metrics[columnHeaders[i]][country] = int(report['rows'][0][i])

        except HttpError as e:
            print("An HTTP error {} occurred:".format(e.resp.status))
            print("{}".format(e.content))

    # print(metrics)
    compute_totals(metrics)
    update_views_pie(metrics)
    update_views_graph(metrics)
    print_sorted_metrics(metrics)
