#!/Library/Frameworks/Python.framework/Versions/3.5/bin/python3

from datetime import datetime, timedelta
import httplib2
# import sys
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

# list used to loop through the client_secrets_files to authenticate and get analytics report
countries = ["AU", "AR", "BR", "DE", "FR", "IT", "MX", "NL", "QC", "RU", "UK"]

client_secrets_files = {
    "AU": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_AU.json",
    "AR": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_AU.json",
    "BR": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_BR.json",
    "DE": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_DE.json",
    "FR": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_FR.json",
    "IT": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_IT.json",
    "MX": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_MX.json",
    "NL": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_NL.json",
    "QC": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_NL.json",
    "RU": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_RU.json",
    "UK": "/Users/kestrel/gitBucket/youtube_analytics/credentials/client_ids/client_id_UK.json"
}

# These OAuth 2.0 access scopes allow for read-only access to the authenticated
# user's account for both YouTube Data API resources and YouTube Analytics Data.
youtube_scopes = ["https://www.googleapis.com/auth/youtube.readonly",
                  "https://www.googleapis.com/auth/yt-analytics.readonly",
                  # "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
                  ]


class AuthenticatedAnalytics(object):
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

        storage = Storage("/Users/kestrel/gitBucket/youtube_analytics/credentials/oaths/{}-{}-oauth2.json".format("analytics_test_multiple_channel.py", oauth_file_path))
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            credentials = run_flow(flow, storage, self.args)

        http = credentials.authorize(httplib2.Http())

        self.youtube = build(self.youtube_api_service_name, self.youtube_api_version, http=http)
        self.youtube_analytics = build(self.youtube_analytics_api_service_name, self.youtube_analytics_api_version, http=http)

    def get_channel_id(self):
        self.channels_list_response = self.youtube.channels().list(mine=True, part="id").execute()

        self.channel_id = self.channels_list_response["items"][0]["id"]

    def run_analytics_report(self):
        '''
        Call the Analytics API to retrieve a report. For a list of available reports,
        see: https://developers.google.com/youtube/analytics/v1/channel_reports
        '''
        self.analytics_query_response = self.youtube_analytics.reports().query(
            ids="channel=={}".format(self.channel_id),
            metrics=self.args.metrics,
            # dimensions=options.dimensions,
            start_date=self.args.start_date,
            end_date=self.args.end_date,
            alt=self.args.alt,
            # max_results=options.max_results,
            sort=self.args.sort
        ).execute()

        return self.analytics_query_response

    def print_report(self, country):
        '''
        parses the analytics API query response - which can be JSON or CSV - and prints relevant info.
        need to pass in country here just for cli printing
        '''
        print("ANALYTICS DATA FOR {}'s CHANNEL".format(country))

        for column_header in self.analytics_query_response.get("columnHeaders", []):
            print("{:<20}".format(column_header["name"]), end='')
        print("")

        for row in self.analytics_query_response.get("rows", []):
            for value in row:
                print("{:<20.0f}".format(value), end='')
        print("\n")


class Analytics(object):
    '''
    hold analytics metrics in a dict in format: {'metric': {'country': integer}}.
    methods to process metrics in various ways, print results, and update plotly graphs
    '''

    def __init__(self):
        self.metrics = {}

    def compute_totals(self):
        for key in self.metrics.keys():
            self.metrics[key]['total'] = sum(value for value in self.metrics[key].values())

    def update_views_pie(self):
        labels = []
        values = []

        for key, value in self.metrics['views'].items():
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

    def update_views_graph(self):
        # this function relies on 'total' being a key in the metrics['views'] dict

        print("updating graph...\n")

        sorted_views = self.sort_metrics('views')
        x = [sorted_views[i][0] for i in range(len(sorted_views)) if sorted_views[i][0] != 'total']
        y = [sorted_views[i][1] for i in range(len(sorted_views)) if sorted_views[i][0] != 'total']
        total_views = 'TOTAL VIEWS: ' + '{:,}'.format(self.metrics['views']['total'])

        views_graph = py.get_figure("https://plot.ly/~allrecipes_international/4/")
        views_graph_annotations = views_graph.layout.annotations
        views_graph_data = views_graph.data

        views_graph_data.update({'x': x, 'y': y})
        views_graph_annotations.update({'text': total_views})

        py.plot(views_graph, filename='youtube channel views graph', auto_open=False)

    def sort_metrics(self, key):

        return sorted(self.metrics[key].items(), key=lambda x: x[1], reverse=True)

    def print_sorted_metrics(self):
        for key in self.metrics.keys():

            print(key.upper())

            sorted_metrics = self.sort_metrics(key)
            for i in range(len(sorted_metrics)):
                print('{}: {:,}'.format(sorted_metrics[i][0], sorted_metrics[i][1]))

            print('\n')


if __name__ == "__main__":

    authenticated_analytics = AuthenticatedAnalytics(client_secrets_files, youtube_scopes)
    authenticated_analytics.parse_cli_arguments()

    # create a list of the metric names in the report from command line arguments passed to --metrics
    # the order of these doesn't matter because the metric names are keys in the metrics dict
    columnHeaders = authenticated_analytics.args.metrics.split(',')

    analytics = Analytics()
    analytics.metrics = {column: {} for column in columnHeaders}

    for country in countries:

        authenticated_analytics.get_authenticated_services(country)

        try:
            authenticated_analytics.get_channel_id()
            report = authenticated_analytics.run_analytics_report()
            authenticated_analytics.print_report(country)

            # columnHeaders = [report['columnHeaders'][i]['name'] for i in range(len(report['columnHeaders']))]

            # update dicts in metrics with country specific values corresponding to a report metric
            for i in range(len(columnHeaders)):
                analytics.metrics[columnHeaders[i]][country] = int(report['rows'][0][i])

        except HttpError as e:
            print("An HTTP error {} occurred:".format(e.resp.status))
            print("{}".format(e.content))

    # print(analytics.metrics)
    analytics.compute_totals()
    analytics.update_views_pie()
    analytics.update_views_graph()
    analytics.print_sorted_metrics()
