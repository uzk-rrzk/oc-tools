#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
Deletes mediapackages from an Opencast installation, even though they are in an inconsistent state
"""

from __future__ import print_function

import argparse
import errno
import getpass
import os
import re
import shutil
import time
import sys
import urlparse

from lxml import etree

import requests
from requests.auth import HTTPDigestAuth
from requests.exceptions import ConnectionError

# List of directories under the given "mountpoint", where distributed mediapackage will be searched
# and retracted from.
DISTRIBUTION_DIRS = [
    'downloads/mh_default_org/engage-player',
    'downloads/mh_default_org/internal',
    'streaming/mh_default_org/engage-player'
]

# List of tuples where the first element is the files' parent directory, relative to the mountpoint,
# and the second is a regexp matching the files to be deleted.
# A wildcard of "{id}" will be substituted by the given MP's ID in the regexp before performing
# the match
DISTRIBUTION_FILES = [
    ('streaming/mh_default_org', '^engage-player_{id}_present')
]

# Statuses that indicate a job is completed, successfully or not
JOB_FINAL_STATUSES = ["FINISHED", "FAILED"]

# Maximum number of attempts to check the status of an Opencast job
MAX_ATTEMPTS = 10

# Seconds between attempts to check the statos of an Opencast job
WAIT_TIME = 5

# Job endpoint
JOB_ENDPOINT = 'services/job/{0}.xml'

# Address of the workflow get endpoint
WF_GET_ENDPOINT = '/workflow/instances.xml'

# Address of the workflow delete endpoint
WF_DELETE_ENDPOINT = '/workflow/remove/{0}'

# Address of the archive endpoint to list existing entries
ARCH_GET_ENDPOINT = 'archive/episode.xml'

# Address of the archive endpoint to delete entries in the archive
ARCH_DELETE_ENDPOINT = 'archive/delete/{0}'

# Address of the legacy archive 'get' endpoint
LEGACY_ARCH_GET_ENDPOINT = 'episode/episode.xml'

# Address of the legacy archive 'delete' endpoint
LEGACY_ARCH_DELETE_ENDPOINT = 'episode/delete/{0}'

# Address of the search "get episodes" endpoint
SEARCH_GET_ENDPOINT = "search/episode.xml"

# Address of the search "delete" endpoint
SEARCH_DELETE_ENDPOINT = 'search/{0}'

# Necessary namespaces
MP_NAMESPACE = "http://mediapackage.opencastproject.org"
WF_NAMESPACE = "http://workflow.opencastproject.org"
JOB_NAMESPACE = "http://job.opencastproject.org"
SEARCH_NAMESPACE = "http://search.opencastproject.org"

XML_MP_TAG = '{{{}}}mediapackage'.format(MP_NAMESPACE)
XML_WF_TAG = '{{{}}}workflow'.format(WF_NAMESPACE)
XML_OP_TAG = '{{{}}}operation'.format(JOB_NAMESPACE)

# XML attribute containing the identifier of the node to which it belongs
XML_ID_ATTR = 'id'
# XML attribute containing the total number of results in a workflow query
XML_WF_TOTAL_ATTR = 'totalCount'
# XML attribute containing a job's current status
XML_JOB_STATUS_ATTR = 'status'

# Name of the query parameter to specify an identifier
QUERY_ID = "id"
# Name of the query paramenter to specify a MP identifier to the workflow service
QUERY_WF_MP = 'mp'
# Name of the query parameter to specify a page size to the workflow service
QUERY_WF_PAGE_SIZE = 'count'
# Name of the query parameter to specify a page offset to the workflow service
QUERY_WF_PAGE_OFFSET = 'startPage'
# Name of the query parameter to compress results provided by the workflow service
QUERY_WF_COMPACT = 'compact'

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
    """
    Makes sure a URI contains all the components, in particular, the protocol.
    If not specified, the protocol defaults to http.
    This function circumvents the assumption, documented by the corresponding RFCs,
    that a string that does not start with "//" does not contain a network location,
    only a relative path. The url provided as argument will always be assumed to
    contain a network location, even though it does not start with a protocol or with
    "//".
    """
    parsed_url = urlparse.urlparse(url, 'http')
    if parsed_url.netloc:
        return urlparse.urlunparse(parsed_url)
    else:
        # The URLs without protocol need to be preceded by // or urlparse will not
        # correctly interpret them
        return urlparse.urlunparse(urlparse.urlparse("//" + url, 'http'))


def wait_for_job(job_id, server_url, auth, mp_id=None):
    """
    Waits while an asynchronous job completes and reports its state
    """
    if job_id < 0:
        return

    if mp_id:
        prefix = "[{}] ".format(mp_id)
    else:
        prefix = ""

    # Check when the job finishes
    for dummy in range(MAX_ATTEMPTS):
        # Give the job some time to finish
        time.sleep(WAIT_TIME)

        # Check the status
        resp = requests.get(
            urlparse.urljoin(server_url, JOB_ENDPOINT.format(job_id)),
            auth=auth
        )

        if resp.status_code == 200:
            job_xml = etree.fromstring(resp.content)
            job_status = job_xml.get(XML_JOB_STATUS_ATTR)
            if job_status in JOB_FINAL_STATUSES:
                print(
                    "{}'{}' operation finished with status: {}".format(
                        prefix,
                        job_xml.find(XML_OP_TAG).text,
                        job_status
                    )
                )
                break
    else:
        if job_status:
            add = ". Last known status is {}".format(job_status)
        else:
            add = ""
        print("{}Timed out waiting for job {} to finish{}".format(prefix, job_id, add))


def unpublish(mp_id, server_url, auth, dry_run):
    """
    Delete publications of the given mediapackage
    """

    try:
        if dry_run:
            resp = requests.get(
                urlparse.urljoin(server_url, SEARCH_GET_ENDPOINT),
                params={QUERY_ID: mp_id},
                auth=auth
            )
            if resp.status_code == 200:
                results = etree.fromstring(resp.content).findall('.//'+XML_MP_TAG)
                if len(results) == 0:
                    print(
                        ("[{}] Would NOT unpublish from search index at {}: "
                         "no publication found").format(
                             mp_id,
                             resp.request.url
                         )
                    )
                else:
                    print(
                        "[{}] Would unpublish from search index at {}".format(
                            mp_id,
                            resp.request.url
                        )
                    )
            else:
                print(
                    "[{}] Not sure if MP is published at {}: server returned unexpected HTTP {}"
                    .format(
                        mp_id,
                        resp.request.url,
                        resp.status_code
                    )
                )
        else:
            resp = requests.delete(
                urlparse.urljoin(server_url, SEARCH_DELETE_ENDPOINT).format(mp_id),
                auth=auth
            )
            print("[{}] Unpublishing MP returned HTTP status {}".format(mp_id, resp.status_code))

            if resp.status_code == 200:
                return int(etree.fromstring(resp.content).get(XML_ID_ATTR))

    except ConnectionError as conn_e:
        print("\nCould not connect to '{0}'.".format(conn_e.request.url), file=sys.stderr)
        print("Please make sure you provided the correct URL and that you are "
              "connected to the internet.", file=sys.stderr, end="\n\n")

    return -1


def retract(mp_id, mountpoint, dry_run):
    """
    Retract distribution files from known locations
    """

    for subdir in DISTRIBUTION_DIRS:
        fulldir = os.path.join(mountpoint, subdir, mp_id)
        if dry_run:
            if os.path.isdir(fulldir) and not os.path.islink(fulldir):
                print("[{}] Would delete distribution directory: '{}'".format(mp_id, fulldir))
        else:
            try:
                shutil.rmtree(fulldir)
                print("[{}] Deleted distribution directory: '{}'".format(mp_id, fulldir))
            except OSError as ose:
                if ose.errno not in [errno.ENOENT, errno.ENOTDIR]:
                    # Ignore the error if the directory does not exists or if the file exists but
                    # it's not a directory
                    raise

    for parent, regexp in DISTRIBUTION_FILES:
        fullparent = os.path.join(mountpoint, parent, '')
        matcher = re.compile(regexp.format(id=mp_id))
        files = []
        try:
            files = os.listdir(fullparent)
        except OSError as ose:
            if ose.errno not in [errno.ENOENT, errno.ENOTDIR]:
                raise

        for filename in files:
            if matcher.match(filename):
                fullpath = urlparse.urljoin(fullparent, filename)
                if dry_run:
                    if os.path.isfile(fullpath):
                        print("[{}] Would delete file '{}'".format(mp_id, fullpath))
                else:
                    try:
                        os.remove(fullpath)
                        print("[{}] Removed distribution file: {}".format(mp_id, fullpath))
                    except OSError as ose:
                        if ose.errno != errno.EISDIR:
                            raise


def delete_workflows(mp_id, server_url, auth, dry_run):
    """
    Delete the workflows corresponding to a certain MP id at the provided server
    """

    wf_get_url = urlparse.urljoin(server_url, WF_GET_ENDPOINT)
    wf_delete_url = urlparse.urljoin(server_url, WF_DELETE_ENDPOINT)

    # Get the query parameters for the WF requests ready
    query_params = dict()
    query_params[QUERY_WF_PAGE_OFFSET] = 0
    query_params[QUERY_WF_COMPACT] = True
    query_params[QUERY_WF_PAGE_SIZE] = DEFAULT_PAGE_SIZE
    query_params[QUERY_WF_MP] = mp_id

    i = 0
    n_workflows = -1
    try:
        while (n_workflows < 0) or (i < n_workflows):
            resp = requests.get(wf_get_url, params=query_params, auth=auth)
            if resp.status_code != 200:
                print(
                    ("Received unexpected HTTP {0} status while reading the workflow list. "
                     "Please check your network, and that the arguments provided are correct")
                    .format(resp.status_code),
                    file=sys.stderr)
                return

            if n_workflows < 0:
                # Set the maximum number of workflows and then convert the results into a list
                workflows = etree.fromstring(resp.content)
                n_workflows = int(workflows.get(XML_WF_TOTAL_ATTR))
                workflows = workflows.findall('.//'+XML_WF_TAG)
            else:
                # Simply convert the results in an list of XML trees
                workflows = etree.fromstring(resp.content).findall('.//'+XML_WF_TAG)

            for workflow in workflows:
                if dry_run:
                    print(
                        "[{}] Would delete workflow with ID {}".format(
                            mp_id,
                            workflow.get(XML_ID_ATTR)
                        )
                    )
                else:
                    resp = requests.delete(
                        wf_delete_url.format(workflow.get(XML_ID_ATTR)),
                        auth=auth
                    )
                    print(
                        "[{}] Deleting workflow '{}' returned HTTP status {}".format(
                            mp_id,
                            workflow.get(XML_ID_ATTR),
                            resp.status_code
                        )
                    )

            query_params[QUERY_WF_PAGE_OFFSET] += 1
            i += len(workflows)
    except ConnectionError as conn_e:
        print("\nCould not connect to '{0}'.".format(conn_e.request.url), file=sys.stderr)
        print(
            "Please make sure you provided the correct URL and that you are "
            "connected to the internet.", file=sys.stderr, end="\n\n")
        return


def unarchive(mp_id, server_url, auth, dry_run, legacy):
    """
    Delete archive of the given mediapackage
    """

    try:
        resp = requests.get(
            urlparse.urljoin(server_url, ARCH_GET_ENDPOINT),
            params={QUERY_ID: mp_id},
            auth=auth
        )

        if resp.status_code == 200:
            archives = etree.fromstring(resp.content).findall('.//'+XML_MP_TAG)

            if len(archives) > 1:
                print(
                    "[{}] WARNING: {} archive entries were found for the mediapackage!".format(
                        mp_id,
                        len(archives)
                    )
                )

            if dry_run:
                if len(archives) == 0:
                    print(
                        "[{}] Would NOT unarchive MP at URL {}: no archive found".format(
                            mp_id,
                            resp.request.url
                        )
                    )
                elif len(archives) == 1:
                    print(
                        "[{}] Would unarchive MP at URL {}".format(
                            mp_id,
                            resp.request.url
                        )
                    )
            else:
                if legacy:
                    resp = requests.delete(
                        urlparse.urljoin(server_url, LEGACY_ARCH_DELETE_ENDPOINT).format(mp_id),
                        auth=auth
                    )
                else:
                    resp = requests.delete(
                        urlparse.urljoin(server_url, ARCH_DELETE_ENDPOINT).format(mp_id),
                        auth=auth
                    )
                print("[{}] Unarchiving MP at {} returned HTTP status {}".format(
                    mp_id, resp.request.url, resp.status_code))
        else:
            print(
                "[{}] Not sure if MP is archived at {}: server returned unexpected HTTP {}".format(
                    mp_id,
                    resp.request.url,
                    resp.status_code
                )
            )
    except ConnectionError as conn_e:
        print("\nCould not connect to '{0}'.".format(conn_e.request.url), file=sys.stderr)
        print("Please make sure you provided the correct URL and that you are "
              "connected to the internet.", file=sys.stderr, end="\n\n")
        return


def delete_mp(args):
    """
    Delete the MP with ID from the Opencast system.

    In particular, this script does the following:

        - Unpublish the MP from the engage player
        - Delete distributed elements from downloads and streaming
        - Delete all workflows associated with this MP
        - Delete the MP from the archive
    """

    # Process server URLs
    admin_url = normalize_url(args.admin_url)

    if args.search_url:
        search_url = normalize_url(args.search_url)
    else:
        search_url = admin_url

    if not args.digest_user:
        setattr(args, "digest_user", raw_input("Enter the digest authentication user: "))
    if not args.digest_pass:
        setattr(args, "digest_pass", getpass.getpass("Enter the digest authentication password: "))

    # Set authentication mechanism
    auth = OpencastDigestAuth(args.digest_user, args.digest_pass)

    if args.mediapackage_id[0] == '@':
        # Asume the ID is a file containing one ID per line
        with open(args.mediapackage_id[1:], 'r') as inputfile:
            for mp_id in inputfile.readlines():
                # Remove whitespace and ignore everything after the first inner whitespace
                mp_id = mp_id.strip().split()[0]
                if mp_id[0] == '#':
                    # Ignore comments
                    continue

                # Wait for the unpublish job to finish
                wait_for_job(
                    unpublish(mp_id, search_url, auth, args.not_really),
                    args.admin_url, auth, mp_id)
                retract(mp_id, args.mountpoint, args.not_really)
                delete_workflows(mp_id, admin_url, auth, args.not_really)
                unarchive(mp_id, admin_url, auth, args.not_really, args.legacy)
    else:
        wait_for_job(
            unpublish(args.mediapackage_id, search_url, auth, args.not_really),
            args.admin_url, auth, args.mediapackage_id)
        retract(args.mediapackage_id, args.mountpoint, args.not_really)
        delete_workflows(args.mediapackage_id, admin_url, auth, args.not_really)
        unarchive(args.mediapackage_id, admin_url, auth, args.not_really, args.legacy)


if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=
        "Delete a MP with the given ID from the Opencast system identified by its server URLs.\n \n"
        "In particular, this script performs the following actions:\n"
        "    - Unpublish the MP from the engage player\n"
        "    - Delete distributed elements from downloads and streaming\n"
        "    - Delete all workflows associated with this MP\n"
        "    - Delete the MP from the archive\n \n"
        "Please note, that this script must run in a machine with access to the network volume\n"
        "containing, at least, the download and streaming directories, because the media\n"
        "retraction is performed directly and not through the endpoints.\n \n"
        "Because this script is designed to delete a mediapackage even if it is in an\n"
        "inconsistent status, each of the actions is performed independently from each other,\n"
        "even though some of them fail for some reason.\n \n"
    )

    # We are only interested in file names, but this way the parser makes sure those files exist
    parser.add_argument(
        'mediapackage_id',
        help='The ID of the mediapackage to be deleted from the cluster')
    parser.add_argument(
        'admin_url',
        help='The URL of the server running the archive and workflow services')
    parser.add_argument(
        'mountpoint',
        help='The directory in the filesystem containing the parent directory\nunder which the '
        'distribution directories (\'download\' and \'streaming\') are.\n'
        'You may want to modify the paths relative to the mountpoint by\nediting this script'
    )
    parser.add_argument(
        'search_url',
        nargs='?',
        help='The URL of the server running the search (publication) service.\n'
        'Defaults to the admin URL')
    parser.add_argument(
        '-l',
        '--legacy',
        action="store_true",
        help="Use the old archive endpoint '/episode' (up to version 2.0),\ninstead of the new one"
        "'/archive' (from version 2.0, inclusive)")
    parser.add_argument(
        '-n',
        '--not_really',
        action="store_true",
        help='Do not delete anything, but show what would be done if this\noption were not provided')
    parser.add_argument(
        '-u',
        '--digest_user',
        help='User to authenticate with the Opencast endpoint in the servers')
    parser.add_argument(
        '-p',
        '--digest_pass',
        help='Password to authenticate with the Opencast endpoint in the servers')

    #print(parser.parse_args())
    #exit(0)

    exit(delete_mp(parser.parse_args()))
