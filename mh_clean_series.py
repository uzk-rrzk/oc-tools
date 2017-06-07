#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function

import sys
import requests
from requests.auth import HTTPDigestAuth
from requests.exceptions import ConnectionError
import urlparse
import argparse
import getpass
from lxml import etree


# Address of the series get endpoint
SERIES_GET_ENDPOINT = 'series/series.json'

# Address of the series delete endpoint
SERIES_DELETE_ENDPOINT = 'series/{0}'

# Address of the archive get endpoint
ARCHIVE_GET_ENDPOINT = 'episode/episode.json'

# Necessary namespaces
MP_NAMESPACE = "http://mediapackage.opencastproject.org"
DC_NAMESPACE = "http://purl.org/dc/terms/"
WF_NAMESPACE = "http://workflow.opencastproject.org"

# Query parameters for the series service
SERIES_QUERY_PAGE_SIZE = 'count'
SERIES_QUERY_PAGE_OFFSET = 'startPage'

# Query parameters for the archive service
ARCHIVE_QUERY_PAGE_SIZE = 'limit'
ARCHIVE_QUERY_SERIES_ID = 'series'

DEFAULT_PAGE_SIZE = 50

class OpencastDigestAuth(HTTPDigestAuth):
    """ Implement a digest authentication including the headers required by Opencast """

    def __call__(self, r):
        # Call the parent method
        r = super(OpencastDigestAuth, self).__call__(r)

        # Add Opencast required headers
        r.headers['X-Requested-Auth'] = 'Digest'
        r.headers['X-Opencast-Matterhorn-Authorization'] = 'true'

        return r


def normalize_url(url):
    parsed_url = urlparse.urlparse(url, 'http')
    if parsed_url.netloc:
        return urlparse.urlunparse(parsed_url)
    else:
        # The URLs without protocol need to be preceded by // for urlparse to interpret them correctly
        return urlparse.urlunparse(urlparse.urlparse("//" + url, 'http'))


def main(args):

    # Process server URLs
    series_url = normalize_url(args.series_url)

    series_get_url = urlparse.urljoin(series_url, SERIES_GET_ENDPOINT)
    series_delete_url = urlparse.urljoin(series_url, SERIES_DELETE_ENDPOINT)

    if args.archive_url:
        archive_get_url = urlparse.urljoin(normalize_url(args.archive_url), ARCHIVE_GET_ENDPOINT)
    else:
        archive_get_url = urlparse.urljoin(series_url, ARCHIVE_GET_ENDPOINT)


    if not args.digest_user:
        setattr(args, "digest_user", raw_input("Enter the digest authentication user: "))
    if not args.digest_pass:
        setattr(args, "digest_pass", getpass.getpass("Enter the digest authentication password: "))

    # Set authentication mechanism
    auth = OpencastDigestAuth(args.digest_user, args.digest_pass)

    # Get the query parameters for the series requests ready
    series_query = {
        SERIES_QUERY_PAGE_OFFSET: 0,
        # Just the first time, to get the total amount of series
        SERIES_QUERY_PAGE_SIZE: 1
    }

    # Get the total number of series
    r = requests.get(series_get_url,params=series_query,auth=auth)
    r.raise_for_status()

    # Get the total number of series
    series_total = int(r.json()['totalCount'])
    
    # Change the page size to a more reasonable amount
    series_query[SERIES_QUERY_PAGE_SIZE] = DEFAULT_PAGE_SIZE

    print()

    # Define the query parameters for the archive service
    archive_query = {
        ARCHIVE_QUERY_PAGE_SIZE: 1
    }
    
    # Main loop
    series_processed = 0
    series_to_delete = []
    while series_processed < series_total:

        # Get some series from the service
        r = requests.get(series_get_url,params=series_query,auth=auth)
        r.raise_for_status()

        for series in [catalog[DC_NAMESPACE] for catalog in r.json()['catalogs']]:
            series_processed += 1

            series_id = series['identifier'][0]['value']

            if not series_id:
                print(u"[WARN] Skipping series with no ID: '{0}'".format(series['title']['value']))
                continue

            # Check if there are archived mediapackages that belong to the series
            archive_query[ARCHIVE_QUERY_SERIES_ID] = series_id
            r = requests.get(archive_get_url, params=archive_query, auth=auth)
            r.raise_for_status()

            # If the number of mediapackages is 0, then we mark the series for deletion
            if int(r.json()['search-results']['total']) == 0:
                series_to_delete.append(series)
            
        series_query[SERIES_QUERY_PAGE_OFFSET] += 1
        print(
            u"\rReading series ({0}/{1} completed)...".format(series_processed, series_total),
            end="")
        sys.stdout.flush()

    print(u" Finished!\n")

    if series_to_delete:
        if args.not_really:
            if args.force:
                print(u"{0} series would be deleted:".format(len(series_to_delete)))
            else:
                print(u"{0} series would be deleted. ".format(len(series_to_delete)), end="")
                answer = raw_input("Do you want to list them? (Y/N) ")

                if not (answer and "yes".startswith(answer.lower())):
                    return 0

            for series in series_to_delete:
                print(
                    u"\t* {0}: '{1}'".format(
                        series['identifier'][0]['value'],
                        series['title'][0]['value'])
                )
        else:
            if not args.force:
                answer = raw_input("{0} series will be deleted. Are you sure? (Y/N) "
                                   .format(len(series_to_delete)))

                if not (answer and "yes".startswith(answer.lower())):
                    print(u"Aborting on user request")
                    return 0
                
            series_deleted = []
            juapa = True
            for series in series_to_delete:
                series_id = series['identifier'][0]['value']
                r = requests.delete(series_delete_url.format(series_id), auth=auth)
                
                series_deleted.append((series,r))

                print(
                    u"\rDeleted ({0}/{1})...".format(
                        len(series_deleted), len(series_to_delete)),
                    end="")
                sys.stdout.flush()

            print(u" Finished!\n")

            for series, r in series_deleted:
                if r.status_code == 204:
                    print(u"Deleted {0}: {1}".format(
                        series['identifier'][0]['value'],
                        series['title'][0]['value']))
                else:
                    print(u"\nFailed {0}: {1}".format(
                        series['identifier'][0]['value'],
                        series['title'][0]['value']))
                    if r.status_code == 404:
                        print(u"\tThe series does not exist or has been already deleted\n")
                    else:
                        print(u"\tReceived unexpected HTTP {0} response\n".format(r.status_code))
    else:
        print("Could not find any empty series")


if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Delete empty series in an Opencast system")

    # We are only interested in file names, but this way the parser makes sure those files exist
    parser.add_argument('series_url', help='The URL of the server running the series service')
    parser.add_argument('archive_url', nargs='?', help='The URL of the server running the archive service. Defaults to the series URL parameter.')
    parser.add_argument('-n', '--not_really', action="store_true", help='Do not delete anything, but show what would be done if this option were not provided')
    parser.add_argument('-f', '--force', action="store_true", help='Do not ask for confirmation to delete the workflows. In combination with \'-n\', do not ask for confirmation to print the workflow IDs')
    parser.add_argument('-u', '--digest_user', help='User to authenticate with the Opencast endpoint in the server')
    parser.add_argument('-p', '--digest_pass', help='Password to authenticate with the Opencast endpoint in the server')
    
    #    print(parser.parse_args())
    #    exit(0)
    
    try:
        main(parser.parse_args())
        sys.exit(0)
    except ConnectionError as e:
        print(u"\nCould not connect to '{0}'.".format(e.request.url), file=sys.stderr)
        print(u"Please make sure you provided the correct URL and that you are connected to the internet.", file=sys.stderr, end="\n\n")
        sys.exit(1)
    except Exception as exc:
        print(u"\nERROR ({0}): {1}".format(type(exc).__name__, exc), file=sys.stderr)
        sys.exit(2)
