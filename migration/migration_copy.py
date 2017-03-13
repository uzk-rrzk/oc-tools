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

# Location of the config.properties file in the Matterhorn server
DEFAULT_CONF_FILE = "/etc/matterhorn/config.properties"
# Address of the search (get) endpoint
DEFAULT_SEARCH_ENDPOINT='/search/episode.xml'

DEFAULT_DIR_NAME="oc_migration"
MANIFEST_NAME="manifest.xml"

# Keys in the Matterhorn configuration that indicate the locations where to look for files
LOCATION_KEYS = [ "org.opencastproject.streaming.directory", "org.opencastproject.download.directory" ]

# The mode applied to the created directories
DIRMODE = 0o755

# Namespace to use at the mediapackages
MP_NS="http://mediapackage.opencastproject.org"

# Namespace to use in search results
SEARCH_NS="http://search.opencastproject.org"


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


# Get the distribution directories from the remote server's configuration
# If the "extra_dirs" argument is a non-empty sequence, add these dirs to the set
def get_dirs(conf_file, keys, extra_dirs):

    dirs = set()
    pattern = re.compile("^\s*({0})\s*=(.*)".format("|".join([re.escape(key) for key in keys])))

    with open(conf_file, "r") as f:
        for line in f:
            match = pattern.match(line)
            if match:
                if match.group(1) not in keys:
                    print("[ERROR] Match of {0} is not in {1}!!".format(match.group(1), keys))
                if match.group(2):
                    dirs.add(match.group(2))
                else:
                    print("[WARNING] '{0}' property is empty".format(key), file=sys.stderr)

    if extra_dirs:
        for d in extra_dirs:
            if os.path.isdir(d):
                dirs.add(d)
    
    return dirs


# Remove the server "mountpoint" from a track's URL
def get_relative_path(url):

    # Get this track's URL and parse it
    url_parsed = urlparse.urlparse(url)

    # Extract the download server "mountpoint"
    # Matterhorn resource URLs in distributed mediapackage take the form:
    #    distribution-channel/mediapackage-id/element-id/filename.extension
    # , therefore, anything that is beyond these four levels in the hierarchy 
    # is a part of the download server "mountpoint"
    url_path = os.path.dirname(url_parsed.path)
    for i in range(3):
        url_path = os.path.dirname(url_path)
                    
    # Remove the "mountpoint" from the resource's path to get the system path
    relative_path = os.path.relpath(url_parsed.path, url_path)

    # If the path contains a "file extension prefix" (streaming URL), move it to the right place
    prefix, sep, path = relative_path.partition(':')
    if sep == ':':
        path, ext = os.path.splitext(path)
        if not ext:
            return path + '.' + prefix
        elif prefix != ext[1:]:
            print("WARNING: Found conflicting extension prefix in path '{0}'. Ignoring the prefix '{1}'".format(relative_path, prefix),
                  file = sys.stderr)
            return path
    else:
        return relative_path


class NotFoundError(Exception):
    pass

class IncorrectCopyError(Exception):
    pass


def main(args):
    
    try:

        # Process server URLs
        search_url = urlparse.urlparse(args.search_url, 'http')
        if not search_url.netloc:
            # The URLs without protocol need to be preceded by // for urlparse to interpret them correctly
            search_url = urlparse.urlparse("//" + args.search_url, 'http')

        if not os.path.isdir(args.inbox_path):
            print("[ERROR] {0} is not a valid inbox directory".format(args.inbox_path), file=sys.stderr)
            return 1

        # Get the directories where to look for the files to download
        dirs = get_dirs(args.config, LOCATION_KEYS, args.extra_dirs)

        # Read the elements published in the search index and create an XML document tree out of the response
        if not args.digest_user:
            print("Enter the digest authentication user: ", file=sys.stderr, end="")
            setattr(args, "digest_user", raw_input())

        if not args.digest_pass:
            setattr(args, "digest_pass", getpass.getpass("Enter the digest authentication password: "))

        try: 
            cousa = curl(search_url,
                         args.search_endpoint,
                         user=args.digest_user, 
                         password=args.digest_pass)

            document = etree.fromstring(cousa)

            working_dir = os.path.join(args.working_base, DEFAULT_DIR_NAME)
            try:
                os.mkdir(working_dir)
            except OSError as oe:
                if not os.path.isdir(working_dir):
                    print("[ERROR] {0} could not be created: {1}".format(working_dir, oe), file=sys.stderr)
                    raise
            
            # For every mediapackage in the results...
            for mp in document.iter('{{{0}}}mediapackage'.format(MP_NS)):
            
                if INTERRUPTED:
                    print('\nInterrupted by user.\n')
                    return(0)

                # Delete attachments
                attachments = mp.find('{{{0}}}attachments'.format(MP_NS))
                for attach in attachments:
                    attachments.remove(attach)
                
                # Delete catalogs that are not dublincore
                metadata = mp.find('{{{0}}}metadata'.format(MP_NS))
                for catalog in metadata:
                    if not catalog.get('type').startswith('dublincore/'):
                        metadata.remove(catalog)

                # The destination directory where the mediapackage's elements will be copied to
                dest_root = os.path.join(working_dir, mp.get('id'))
                
                if os.path.isfile(os.path.join(dest_root, mp.get('id') + '.zip')):
                    print("Skip mediapackage {0}: zip file already exists".format(mp.get('id')))
                    continue
                                 
                # This is helpful to resume a failed copy
                copied_paths = set()

                try:
                    # Iterate through the URLs in this mediapackage
                    for url in mp.iter('{{{0}}}url'.format(MP_NS)):

                        # Get the relative path of the resource in the remote server and split it in two parts
                        relative_path = get_relative_path(url.text)
                        
                        relative_dir, element_name = os.path.split(relative_path)
                    
                        # element_dir is the (relative) directory containing the element to copy
                        # element_root is the (relative)  directory containing element_dir
                        # short_path is the subpath including element_dir and element_name
                        element_root, element_dir = os.path.split(relative_dir)
                        short_path = os.path.join(element_dir, element_name)

                        # Try to find the relative path in one of the directories read in the configuration
                        for src_root in dirs:
                            try:
                                absolute_src_root = os.path.join(src_root, element_root)
                                absolute_dest_dir = os.path.join(dest_root, element_dir)
                                # We need to copy only the element and the directory containing it
                                # Therefore we need to set the working directory to the parent directory of such directory
                                os.chdir(absolute_src_root)
                                shutil.copytree(element_dir, absolute_dest_dir)
                                
                                # Directory successfully copied
                                # We assume the first correct copy is the only one possible, so we break
                                #print("Copied {0} to {1}".format(os.path.join(absolute_src_root, element_dir), absolute_dest_dir))

                                # Mark the path as copied
                                copied_paths.add(short_path)
                                # Fix the url in the manifest
                                url.text = short_path
                                break

                            except (IOError, shutil.Error, OSError) as e:
                                abs_src_path = os.path.join(absolute_src_root, short_path)
                                abs_dst_path = os.path.join(dest_root, short_path)
                                if os.path.isfile(abs_src_path):
                                    if os.path.isfile(abs_dst_path):
                                        if filecmp.cmp(abs_src_path, abs_dst_path):
                                            if short_path in copied_paths:
                                                # Delete the duplicated element (this can be done, because the url elements are always nested so that they have a "grandparent" element)
                                                url.getparent().getparent().remove(url.getparent())
                                                break
                                            else:
                                                print("[WARN] Element {0} was already copied. Maybe from a previous run of the script?"
                                                      .format(abs_dst_path), file=sys.stderr)
                                                # Mark the path as copied
                                                copied_paths.add(short_path)
                                                # Fix the url in the manifest
                                                url.text = short_path
                                                break
                                        else:
                                            raise IncorrectCopyError("Element '{0}' was already copied but was different than the source candidate '{1}'"
                                                                     .format(abs_dst_path, abs_src_path))

                        else:
                            raise NotFoundError("Could not copy the URL {0} from the standard locations".format(url.text))

                        
                except (IncorrectCopyError, NotFoundError) as e:
                    print("[ERROR] Skipping mediapackage {0}: {1}".format(mp.get('id'), e), file=sys.stderr)
                    continue

                # All URLs have been processed without errors
                # Serialize the manifest
                with open(os.path.join(dest_root, MANIFEST_NAME), "w+") as f:
                    etree.ElementTree(mp).write(f, encoding="utf-8", xml_declaration=True, pretty_print=True)
                            
                # Zip the mediapackage
                os.chdir(dest_root)
                subprocess.check_call([ 'zip', '-0r', mp.get('id') ] + os.listdir(dest_root))

                # Copy the mediapackage in the inbox
                #shutil.copy(mp.get('id') + '.zip', args.inbox_path)

                print("Mediapackage {0} successfully zipped".format(mp.get('id')))
                #sleep(60)
                break

        except RuntimeError as re:
            print("[ERROR] Skipping mediapackage {0} - {2}: {1}".format(mp.get('id'), re, type(re).__name__), file=sys.stderr)

        except (IOError, shutil.Error) as e:
            print("[ERROR] Could not copy element(s): {0}".format(e), file=sys.stderr)
                
        except subprocess.CalledProcessError as cpe:
            print("[ERROR] Could not zip the mediapackage {0}: command '{1}', error {2}".format(mp.get('id'), cpe.cmd, cpe.returncode), 
                  file=sys.stderr)

        except pycurl.error as err:
            print("[ERROR] Could not get the list of published mediapackages: {0}".format(err), file=sys.stderr)
            return 1

    except Exception as exc:
        print("[ERROR] {0}: {1}".format(type(exc).__name__, exc), file=sys.stderr)
        return 1


if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Migrate Opencast videos using the inbox")
    
    parser.add_argument('search_url', help='The URL of the Matterhorn server running a "search" service, from where the list of published videos will be obtained')
    parser.add_argument('inbox_path', help='The URL where the inbox is, i.e. where the zip files will be copied')
    parser.add_argument('working_base', help='The directory where this script\'s working directory will be created.')
    parser.add_argument('-c', '--config', default=DEFAULT_CONF_FILE, help='Absolute path of the Matterhorn configuration file in the remote server. (Default: ''{0}'')'.format(DEFAULT_CONF_FILE))
    parser.add_argument('-e', '--search_endpoint', default=DEFAULT_SEARCH_ENDPOINT,
                        help='Endpoint, relative to the search_url, that should return the published mediapackages. (Default: ''{0}'')'
                        .format(DEFAULT_SEARCH_ENDPOINT))
    parser.add_argument('-u', '--digest_user', help='User to authenticate with the Matterhorn endpoint in the server')
    parser.add_argument('-p', '--digest_pass', help='Password to authenticate with the Matterhorn endpoint in the server')
    parser.add_argument('-d', '--directory', action="append", dest="extra_dirs",
                        help='Add an additional directory where the media files will be searched for. Can be specified several times.\n\
                        Please note that the directories \'download.dir\' and \'streaming.dir\' in the Matterhorn server configuration \
                        will always be inspected by default')

    sys.exit(main(parser.parse_args()))

