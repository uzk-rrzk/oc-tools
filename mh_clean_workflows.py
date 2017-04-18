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


# Allowed Workflow states
# "Failing" and "running" are not included because they are transient states
WF_VALID_STATES = [ 'instantiated', 'stopped', 'paused', 'succeeded', 'failed' ]

# Address of the workflow get endpoint
WF_GET_ENDPOINT='/workflow/instances.xml'

# Address of the workflow delete endpoint
WF_DELETE_ENDPOINT='/workflow/remove/{0}'

# Necessary namespaces
MP_NAMESPACE="http://mediapackage.opencastproject.org"
WF_NAMESPACE="http://workflow.opencastproject.org"

QUERY_WF_PAGE_SIZE = 'count'
QUERY_WF_PAGE_OFFSET = 'startPage'
QUERY_WF_COMPACT = 'compact'
QUERY_WF_STATE = 'state'

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

    try:
        # Process server URL
        workflow_url = normalize_url(args.workflow_url)

        wf_get_url = urlparse.urljoin(workflow_url, WF_GET_ENDPOINT)
        wf_delete_url = urlparse.urljoin(workflow_url, WF_DELETE_ENDPOINT)

        if not args.digest_user:
            setattr(args, "digest_user", raw_input("Enter the digest authentication user: "))
        if not args.digest_pass:
            setattr(args, "digest_pass", getpass.getpass("Enter the digest authentication password: "))

        # Set authentication mechanism
        auth = OpencastDigestAuth(args.digest_user, args.digest_pass)

        # Get the query parameters for the WF requests ready
        query_params = dict()
        query_params[QUERY_WF_PAGE_OFFSET] = 0
        query_params[QUERY_WF_COMPACT] = True
        query_params[QUERY_WF_STATE] = args.states
        # Just the first time, to get the total amount of WFs
        query_params[QUERY_WF_PAGE_SIZE] = 1

        # Get the total number of workflows
        r = requests.get(wf_get_url,params=query_params,auth=auth)
        if r.status_code != 200:
            print("Received unexpected HTTP {0} status while reading the workflow list. Please check your network, and that the arguments provided are correct"
                  .format(r.status_code),
                  file=sys.stderr)
            return 1

        # Convert the results in an XML DOM
        wf_response = etree.fromstring(r.content)

        # Get the total number of workflows
        wf_total = int(wf_response.get('totalCount'))

        # Change the page size to a more reasonable amount
        query_params[QUERY_WF_PAGE_SIZE] = DEFAULT_PAGE_SIZE

        print()

        # Main loop
        wf_processed = 0
        wf_to_delete = {}
        while wf_processed < wf_total:

            # Get some workflows from the service
            r = requests.get(wf_get_url,params=query_params,auth=auth)

            if r.status_code != 200:
                print("Received unexpected HTTP {0} status while reading the workflow list. Please check your network, and that the arguments provided are correct"
                      .format(r.status_code),
                      file=sys.stderr)
                return 1

            # Convert the results in an XML DOM
            wf_response = etree.fromstring(r.content)

            for wf in wf_response.iter('{{{0}}}workflow'.format(WF_NAMESPACE)):
                wf_processed += 1

                wf_id = wf.get('id')
                wf_state = wf.get('state')
                mp = wf.find('{{{0}}}mediapackage'.format(MP_NAMESPACE))
                mp_id = mp.get('id')

                if mp_id in wf_to_delete:
                    wf_to_delete[mp_id]['workflows'].append((wf_id, wf_state))
                else:
                    # Get MP's title
                    mp_title = mp.find('{{{0}}}title'.format(MP_NAMESPACE))
                    if mp_title is not None:
                        mp_title = mp_title.text
                    else:
                        mp_title = "N/A"

                    # Store this MP id with its title (for easy identification)
                    # and a list of the corresponding workflows
                    wf_to_delete[mp_id]={ "title": mp_title, "workflows": [ (wf_id, wf_state) ] }

            query_params[QUERY_WF_PAGE_OFFSET] += 1
            print("\rReading workflows ({0}/{1} completed)...".format(wf_processed, wf_total), end="")
            sys.stdout.flush()

        print(" Finished!\n")

        if wf_to_delete:
            if args.not_really:
                if args.force:
                    print("{0} workflows would be deleted:".format(wf_processed))
                else:
                    print("{0} workflows would be deleted. ".format(wf_processed), end="")
                    answer = raw_input("Do you want to list their IDs? (Y/N) ")

                    if not (answer and "yes".startswith(answer.lower())):
                        return 0

                for mp_id, data in wf_to_delete.iteritems():
                    print(u"\t* '{1}' ({0}):".format(mp_id, data['title']))
                    for wf_id, state in data['workflows']:
                        print(u"\t\t- {0}, {1}".format(wf_id, state))
                print()
            else:
                if not args.force:
                    answer = raw_input("{0} workflows will be deleted. Are you sure? (Y/N) "
                                       .format(wf_processed))

                    if not (answer and "yes".startswith(answer.lower())):
                        print("Aborting on user request")
                        return 0

                wf_deleted = []
                for mp_id, data in wf_to_delete.iteritems():
                    for wf_id, state in data['workflows']:
                        r = requests.delete(wf_delete_url.format(wf_id),
                                            auth=auth)
                        wf_deleted.append((wf_id,r))

                    print("\rDeleting ({0}/{1})...".format(len(wf_deleted), wf_processed), end="")
                    sys.stdout.flush()

                print(" Finished!\n")

                for wf_id, r in wf_deleted:
                    if r.status_code == 204:
                        print("Workflow {0}: Deleted".format(wf_id))
                    elif r.status_code == 404:
                        print("Workflow {0}: the workflow does not exist or it has been already deleted"
                              .format(wf_id))
                    else:
                        print("Workflow {0}: delete request received an unexpected HTTP {1} response"
                              .format(wf_id, r.status_code))
                print()
        else:
            print("Could not find any workflows with the specified states")
    except ConnectionError as e:
        print("\nCould not connect to '{0}'.".format(e.request.url), file=sys.stderr)
        print("Please make sure you provided the correct URL and that you are connected to the internet.", file=sys.stderr, end="\n\n")
        return 1
    except Exception as exc:
        print(u"\nERROR ({0}): {1}".format(type(exc).__name__, exc), file=sys.stderr)
        return 1


def lower_str(str):
    """
    Returns a lowercase string. This is to make the checks for the correct workflow states case insensitive
    """
    return str.lower()

if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Delete workflows based on their state")

    # We are only interested in file names, but this way the parser makes sure those files exist
    parser.add_argument('workflow_url', help='The URL of the server running the workflow service')
    parser.add_argument('states', nargs='+', type=lower_str, choices=WF_VALID_STATES, help='A list of space-separated workflow states that shall be deleted')
    parser.add_argument('-n', '--not_really', action="store_true", help='Do not delete anything, but show what would be done if this option were not provided')
    parser.add_argument('-f', '--force', action="store_true", help='Do not ask for confirmation to delete the workflows. In combination with \'-n\', do not ask for confirmation to print the workflow IDs')
    parser.add_argument('-u', '--digest_user', help='User to authenticate with the Opencast endpoint in the server')
    parser.add_argument('-p', '--digest_pass', help='Password to authenticate with the Opencast endpoint in the server')

#    print(parser.parse_args())
#    exit(0)

    sys.exit(main(parser.parse_args()))
