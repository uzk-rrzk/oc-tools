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


# Address of the workflow get endpoint
WF_GET_ENDPOINT='/workflow/instances.xml'

# Address of the workflow delete endpoint
WF_DELETE_ENDPOINT='/workflow/remove/{0}'

# Address of the archive endpoint
ARCH_GET_ENDPOINT='archive/episode.xml'

# Address of the legacy archive endpoint
LEGACY_ARCH_GET_ENDPOINT='episode/episode.xml'

# Necessary namespaces
MP_NAMESPACE="http://mediapackage.opencastproject.org"
WF_NAMESPACE="http://workflow.opencastproject.org"
SEARCH_NAMESPACE="http://search.opencastproject.org"

QUERY_WF_PAGE_SIZE = 'count'
QUERY_WF_PAGE_OFFSET = 'startPage'
QUERY_WF_COMPACT = 'compact'

DEFAULT_PAGE_SIZE = 50

# Name of the query parameter to specify the series ID
QUERY_ARCH_MP_ID = "id"


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
    
        if args.archive_url:
            archive_url = normalize_url(args.archive_url)
        else:
            archive_url = workflow_url
    
        wf_get_url = urlparse.urljoin(workflow_url, WF_GET_ENDPOINT)
        wf_delete_url = urlparse.urljoin(workflow_url, WF_DELETE_ENDPOINT)
    
        if args.legacy:
            arch_get_url = urlparse.urljoin(archive_url, ARCH_GET_ENDPOINT)
        else:
            arch_get_url = urlparse.urljoin(archive_url, LEGACY_ARCH_GET_ENDPOINT)
    
        if not args.digest_user:
            setattr(args, "digest_user", raw_input("Enter the digest authentication user: "))
        if not args.digest_pass:
            setattr(args, "digest_pass", getpass.getpass("Enter the digest authentication password: "))
    
        # Set authentication mechanism
        auth = OpencastDigestAuth(args.digest_user, args.digest_pass)
    
        # Dictionaries to hold the lists of archived and non-archived mediapackages
        mp_archived = dict()
        mp_notarchived = dict()
    
        # Get the query parameters for the WF requests ready
        query_params = dict()
        query_params[QUERY_WF_PAGE_OFFSET] = 0
        query_params[QUERY_WF_COMPACT] = True
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
    
        # Main loop
        print()
        wf_notarchived = 0
        wf_processed = 0
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
                mp = wf.find('{{{0}}}mediapackage'.format(MP_NAMESPACE))
                mp_id = mp.get('id')
    
                if mp_id in mp_archived:
                    mp_archived[mp_id]['workflows'].append(wf_id)
                elif mp_id in mp_notarchived:
                    wf_notarchived += 1
                    mp_notarchived[mp_id]['workflows'].append(wf_id)
                else:
                    # Try and find an archived mediapackage with the same ID
                    r = requests.get(arch_get_url, \
                                     params={ QUERY_ARCH_MP_ID: mp_id }, \
                                     auth = auth)
    
                    if r.status_code == 200:
                        # This does not mean we found a match. It can be an empty list...
                        arch_response = etree.fromstring(r.content)
                        arch_list = arch_response.findall('{{{0}}}result'.format(SEARCH_NAMESPACE))
                        if arch_list:
                            # We found a match
    
                            # Get the title of the archived copy of the MP
                            mp_title = arch_response.find('.//{{{0}}}title'.format(MP_NAMESPACE))
                            if mp_title is not None:
                                mp_title = mp_title.text
                            else:
                                mp_title = "N/A"
    
                            # We will store the info in the "archived" dictionary
                            store_it_here = mp_archived
    
                            if len(arch_list) > 1:
                                # This should not happen, but we want to know if it does
                                print("Episode result list had an unexpected length for MP {0}:\
                                {1}".format(mp_id, len(arch_list)))
    
                        else:
                            # Since we have not archive, get the title from the WF instance
                            mp_title = mp.find('{{{0}}}title'.format(MP_NAMESPACE))
                            if mp_title is not None:
                                mp_title = mp_title.text
                            else:
                                mp_title = "N/A"
                                # We will store the info in the "not archived" dictionary
                            store_it_here = mp_notarchived
                            wf_notarchived += 1
    
                        # Store this MP id with its title (for easy identification)
                        # and a list of the corresponding workflows
                        store_it_here[mp_id]={ "title": mp_title, "workflows": [ wf_id ] }
                    elif r.status_code == 404:
                        # The endpoint is incorrect. Issue an error and return
                        if args.legacy:
                            print("Could not find episode endpoint '{0}'"
                                  .format(LEGACY_ARCH_GET_ENDPOINT), file=sys.stderr)
                            print("You are using the '--legacy' option, but the Opencast system version seems to be 2.0 or above.\nPlease remove the '--legacy' option and try again.", file=sys.stderr)
                        else:
                            print("Could not find archive endpoint '{0}'"
                                  .format(ARCH_GET_ENDPOINT), file=sys.stderr)
                            print("It seems you are using an version of Opencast or Matterhorn under 2.0.\nPlease try again with the '--legacy' option",
                                  file=sys.stderr)
                        return 1
                    else:
                        print("Unexpected error HTTP {0}. Please check your network and try \
                        again".format(r.status_code), file=sys.stderr)
                        return 1
    
            query_params[QUERY_WF_PAGE_OFFSET] += 1
            print("\rReading workflows ({0}/{1} completed)...".format(wf_processed, wf_total), end="")
            sys.stdout.flush()
    
        print(" Finished!\n")
    
        if mp_notarchived:
            if args.not_really:
                wf_to_delete = []
                for mp_id, data in mp_notarchived.iteritems():
                    wf_to_delete += data['workflows']

                if args.force:
                    print("{0} workflows would be deleted:".format(len(wf_to_delete)))
                else:
                    print("{0} workflows would be deleted. ".format(len(wf_to_delete)), end="")
                    answer = raw_input("Do you want to list their IDs? (Y/N) ")

                    if not (answer and "yes".startswith(answer.lower())):
                        return 0
                    

                for wf_id in wf_to_delete:
                    print("\t*", wf_id)

                print()
            else:
                if not args.force:
                    answer = raw_input("{0} workflows will be deleted. Are you sure? (Y/N) "
                                       .format(wf_notarchived))
                
                    if not (answer and "yes".startswith(answer.lower())):
                        print("Aborting on user request")
                        return 0
        
                wf_deleted = []
                for mp_id, data in mp_notarchived.iteritems():
                    for wf_id in data['workflows']:
                        r = requests.delete(wf_delete_url.format(wf_id),
                                            auth=auth)
                        wf_deleted.append((wf_id,r))
        
                    print("\rDeleting ({0}/{1})...".format(len(wf_deleted), wf_notarchived), end="")
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
            print("Could not find any workflows of non-archived mediapackages")
    except ConnectionError as e:
        print("\nCould not connect to '{0}'.".format(e.request.url), file=sys.stderr)
        print("Please make sure you provided the correct URL and that you are connected to the internet.", file=sys.stderr, end="\n\n")
        return 1
    except Exception as e:
        print(u"\nERROR ({0}): {1}".format(type(e).__name__, e), file=sys.stderr)
        return 1
    

if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Delete all workflows that belong to mediapackages that are not/no longer archived")

    # We are only interested in file names, but this way the parser makes sure those files exist
    parser.add_argument('workflow_url', help='The URL of the server running the workflow service')
    parser.add_argument('archive_url', nargs='?', help='The URL of the server running the archive service. Defaults to the workflow service URL')
    parser.add_argument('-l', '--legacy', action="store_true", help="Use the old archive endpoint '/episode' (up to version 2.0), instead of the new one '/archive' (from version 2.0, inclusive)")
    parser.add_argument('-n', '--not_really', action="store_true", help='Do not delete anything, but show what would be done if this option were not provided')
    parser.add_argument('-f', '--force', action="store_true", help='Do not ask for confirmation to delete the workflows. In combination with \'-n\', do not ask for confirmation to print the workflow IDs')
    parser.add_argument('-u', '--digest_user', help='User to authenticate with the Opencast endpoint in the server')
    parser.add_argument('-p', '--digest_pass', help='Password to authenticate with the Opencast endpoint in the server')

#    print(parser.parse_args())
#    exit(0)

    sys.exit(main(parser.parse_args()))
