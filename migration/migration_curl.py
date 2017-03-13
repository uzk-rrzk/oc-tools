#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function

import sys    
import pycurl
import urlparse
import urllib
import argparse
import os
import re
import getpass
import signal
import shutil
import subprocess
import filecmp

from StringIO import StringIO 
from lxml import etree

from socket import gaierror
from time import sleep


# Address of the episode (get) endpoint
DEFAULT_EPISODE_ENDPOINT='/episode/episode.xml'

# Address of the ingest (post) endpoint
DEFAULT_INGEST_ENDPOINT='/ingest/addZippedMediaPackage'

# Name of the default workflow definition to be applied
DEFAULT_WF_DEFINITION='migration'

# Namespace to use at the mediapackages
MP_NS="http://mediapackage.opencastproject.org"

# Namespace to use in search results
SEARCH_NS="http://search.opencastproject.org"

# Name of the file used to "mark" the ingested packages
INGESTED=".ingested"

INTERRUPTED=False
# Handle keyboard interrupts gracefully
def signal_handler(signal, frame):
    INTERRUPTED=True

signal.signal(signal.SIGINT, signal_handler)


def curl(server, endpoint, path_params={}, query_params={}, post_params=[], user="", password="", write_to = None, urlencode=True, timeout=None, valid_responses=[200]):
        
    c = pycurl.Curl()

    if write_to is None:
        b = StringIO()
    else:
        b = write_to

    try:
        url = list(server)
            
        url[2] = endpoint.format(**path_params)
        url[4] = urllib.urlencode(query_params)

        c.setopt(pycurl.URL, urlparse.urlunparse(url))
    
        c.setopt(pycurl.FOLLOWLOCATION, False)
        c.setopt(pycurl.CONNECTTIMEOUT, 2)
        if timeout is not None: 
            c.setopt(pycurl.TIMEOUT, int(timeout))
        c.setopt(pycurl.NOSIGNAL, 1)
        c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
        c.setopt(pycurl.USERPWD, user + ':' + password)
        c.setopt(pycurl.HTTPHEADER, ['X-Requested-Auth: Digest', 'X-Opencast-Matterhorn-Authorization: true'])
       
        if post_params:
            c.setopt(pycurl.POST, 1) 
            if urlencode:
                c.setopt(pycurl.POSTFIELDS, urllib.urlencode(post_params))
            else:
                c.setopt(pycurl.HTTPPOST, post_params)

        c.setopt(pycurl.WRITEFUNCTION, b.write)

        #c.setopt(pycurl.VERBOSE, True)

        c.perform()

        status_code = c.getinfo(pycurl.HTTP_CODE)

        if status_code not in valid_responses:
            raise IOError('cURL error in {0}, HTTP status code {1}'.format(urlparse.urlunparse(url), status_code))

        if write_to is None:
            return b.getvalue()

    except Exception as e:
        #print("An exception has occurred: {}, {}".format(type(e).__name__, e), file=sys.stderr)
        raise
    finally:
        c.close() 
        if write_to is None:
            b.close()


def main(args):
    
    try:

        # Process server URLs
        ingest_url = urlparse.urlparse(args.ingest_url, 'http')
        if not ingest_url.netloc:
            # The URLs without protocol need to be preceded by // for urlparse to interpret them correctly
            ingest_url = urlparse.urlparse("//" + args.ingest_url, 'http')

        episode_url_str = args.episode_url if args.episode_url else args.ingest_url
        episode_url = urlparse.urlparse(episode_url_str, 'http')
        if not episode_url.netloc:
            # The URLs without protocol need to be preceded by // for urlparse to interpret them correctly
            episode_url = urlparse.urlparse("//" + episode_url_str, 'http')

        if not os.path.isdir(args.src_dir):
            print("[ERROR] {0} is not a directory".format(args.src_dir), file=sys.stderr)
            return 1

        # Read the digest user and password
        if not args.digest_user:
            print("Enter the digest authentication user: ", end='', file=sys.stderr)
            setattr(args, "digest_user", raw_input())

        setattr(args, "digest_pass", getpass.getpass("Enter the digest authentication password: "))

        for filename in os.listdir(args.src_dir):

            if INTERRUPTED:
                print('\nInterrupted by user.\n')
                return(0)
            
            # Its a directory and it has not been ingested check if if has been ingested
            if not os.path.isdir(os.path.join(args.src_dir, filename)):
                print("[ERROR] {0} is not a directory!".format(filename), file=sys.stderr)
                continue

            ingested_flag = os.path.join(args.src_dir, filename, INGESTED)
            if os.path.exists(ingested_flag):
                print("Skipping already ingested MP {0}".format(filename))
                continue
               
            zip_file = os.path.join(args.src_dir, filename, filename + '.zip')
            if not os.path.exists(zip_file):
                print("[ERROR] The zip file {0} does not exist!".format(zip_file), file=sys.stderr)
                continue
            
            try:
                # Check if it is archived
                response = curl(episode_url,
                                args.episode_endpoint,
                                user=args.digest_user, 
                                password=args.digest_pass,
                                query_params = {'id': filename})
                    
                document = etree.fromstring(response)
                
                # Prepare form parameters
                pars = [ (u'workflowDefinitionId', args.workflow_definition), 
                         (u'archiveOp', str(int(document.get('total')) == 0)),
                         (u'track', (pycurl.FORM_FILE, zip_file)) ]

                response= curl(ingest_url,
                               args.ingest_endpoint,
                               user=args.digest_user, 
                               password=args.digest_pass,
                               post_params = pars,
                               urlencode=False)

                # Mark as ingested
                with open(ingested_flag, 'a'):
                    pass
                    
                print("Successfully ingested MP {0}".format(filename))

                break
            except IOError as err:
                print("[ERROR] Curl error with the mediapackage {1}: {0}".format(err, filename), file=sys.stderr)
                break

    except Exception as exc:
        print("[ERROR] {0}: {1}".format(type(exc).__name__, exc), file=sys.stderr)
        return 1


if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Migrate videos from an Opencast system to other using the REST endpoints")
    
    # We are only interested in file names, but this way the parser makes sure those files exist
    parser.add_argument('src_dir', help='The directory where the stored mediapackages are')
    parser.add_argument('ingest_url', help='The URL of the Matterhorn server running the "ingest" service')
    parser.add_argument('-E', '--episode_url', 
                        help="The URL of the Matterhorn server running the 'episode' service, to check whether a certain MP is already archived. (Default: INGEST_URL)")
    parser.add_argument('-i', '--ingest_endpoint', default=DEFAULT_INGEST_ENDPOINT,
                        help='Endpoint, relative to the ingest_url, that should allow for ingesting zipped mediapackages. (Default: ''{0}'')'
                        .format(DEFAULT_INGEST_ENDPOINT))
    parser.add_argument('-e', '--episode_endpoint', default=DEFAULT_EPISODE_ENDPOINT,
                        help='Endpoint, relative to the episode_url, that should return the archived mediapackages. (Default: ''{0}'')'
                        .format(DEFAULT_EPISODE_ENDPOINT))
    parser.add_argument('-u', '--digest_user', help='User to authenticate with the Matterhorn endpoint in the server')
    parser.add_argument('-w', '--workflow_definition', help='The name of a workflow definition in the destination Matterhorn server, which will be applied to the ingested MP',
                        default=DEFAULT_WF_DEFINITION)

    sys.exit(main(parser.parse_args()))

