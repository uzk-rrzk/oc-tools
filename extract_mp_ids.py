#! /usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import pycurl
import argparse
import getpass
from StringIO import StringIO
from urlparse import urljoin
from lxml import etree

# Address of the search (get) endpoint
SEARCH_ENDPOINT='/search/episode.xml'

# Address of the episode (get) endpoint
EPISODE_ENDPOINT= 'episode/episode.xml'


def main(argv=None):

    c = pycurl.Curl()
    b = StringIO()

    if argv.service == 'search':
        endpoint = SEARCH_ENDPOINT
    else:
        endpoint = EPISODE_ENDPOINT
        
    try: 
        # Read the elements published in the search index
        c.setopt(pycurl.URL, urljoin(argv.url, endpoint))
        c.setopt(pycurl.FOLLOWLOCATION, False)
        c.setopt(pycurl.CONNECTTIMEOUT, 2)
        c.setopt(pycurl.NOSIGNAL, 1)
        c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
        if not argv.user:
            argv.user = raw_input("Enter digest user [{}]: ".format(getpass.getuser()))
            if not argv.user:
                argv.user = getpass.getuser()
            argv.password= getpass.getpass()

        if not argv.password:
            argv.password = getpass.getpass()

        c.setopt(pycurl.USERPWD, argv.user + ':' + argv.password)
        c.setopt(pycurl.HTTPHEADER, ['X-Requested-Auth: Digest', 'X-Opencast-Matterhorn-Authorization: true'])
        c.setopt(pycurl.WRITEFUNCTION, b.write)
        c.setopt(pycurl.VERBOSE, False)
        c.perform()
        status_code = c.getinfo(pycurl.HTTP_CODE)
        
        if status_code == 200:
            # Create an XML document tree out of the response
            document = etree.fromstring(b.getvalue())
            
            # Return all the MP ids in the result
            try:
                if argv.output == '-':
                    f = sys.stdout
                else:
                    f = open(argv.output, 'w+')

                for result in document.iter('{*}result'):
                    f.write(result.get('id') + u'\n')
            finally:
                if argv.output != '-':
                    f.close()
        else:
            print b.getvalue()

    except pycurl.error as err:
        raise RuntimeError(c.errstr())
    except Exception as exc:
        print type(exc), exc
        raise
    finally:
        c.close()
        b.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Extract Mediapackage IDs')
    parser.add_argument('service', help="The service we want to list the URLs from", choices=['episode', 'search'])
    parser.add_argument('url', help='URL of the server running the selected service')
    parser.add_argument('output', default="-", nargs='?', help='An output file to write the result to')
    parser.add_argument('-u', '--user', help='The digest user to access the Opencast endpoint')
    parser.add_argument('-p', '--password', help='The digest password to access the Opencast endpoint')

    sys.exit(main(parser.parse_args()))
