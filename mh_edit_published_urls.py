#!/usr/bin/env python
# -*- coding:utf-8 -*-

import argparse
import getpass
import sys
import pycurl
from StringIO import StringIO
from urlparse import urljoin, urlparse, urlunparse, ParseResult
from urllib import quote_plus
from lxml import etree
from os import path


# The URLs with these protocols will not be modified
EXCLUDE_PROTO=[ 'rtmp', 'rtmps', 'rtmpt', 'rtsp' ]

# Address of the default search (get) endpoint
DEFAULT_SEARCH_ENDPOINT='/search/episode.xml'

# Address of the search (post) endpoint
DEFAULT_ADD_ENDPOINT='search/add'

# Namespace to use at the mediapackages
MP_NAMESPACE="http://mediapackage.opencastproject.org"


def curl(mp_file, server, endpoint, user, password):
    postparams = [ (u'mediapackage', quote_plus(mp_file)) ]

    c = pycurl.Curl()
    b = StringIO()
    #print urljoin(server, endpoint)
    c.setopt(pycurl.URL, urljoin(server, endpoint))
    c.setopt(pycurl.FOLLOWLOCATION, False)
    c.setopt(pycurl.CONNECTTIMEOUT, 2)
    c.setopt(pycurl.NOSIGNAL, 1)
    c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
    c.setopt(pycurl.USERPWD,  user + ':' + password)
    c.setopt(pycurl.HTTPHEADER, ['X-Requested-Auth: Digest', 'X-Opencast-Matterhorn-Authorization: true'])
    c.setopt(pycurl.HTTPPOST, postparams)
    c.setopt(pycurl.WRITEFUNCTION, b.write)
    #c.setopt(pycurl.VERBOSE, True)
    try:
        c.perform()
    except:
        raise RuntimeError, 'connect timed out!'
    status_code = c.getinfo(pycurl.HTTP_CODE)
    print status_code
    c.close() 
    #print b.getvalue()



def main(argv=None):

    c = pycurl.Curl()
    b = StringIO()

    # Filters out the server address
    dls = urlparse(argv.new_dls)

    if not argv.user:
        setattr(argv, "user", raw_input("Enter the digest authentication user: "))
    if not argv.password:
        setattr(argv, "password", getpass.getpass("Enter the digest authentication password: "))

    try:
        # Read the elements published in the search index
        c.setopt(pycurl.URL, urljoin(argv.search_url, argv.search_endpoint))
        c.setopt(pycurl.FOLLOWLOCATION, False)
        c.setopt(pycurl.CONNECTTIMEOUT, 2)
        c.setopt(pycurl.NOSIGNAL, 1)
        c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
        c.setopt(pycurl.USERPWD, argv.user + ':' + argv.password)
        c.setopt(pycurl.HTTPHEADER, ['X-Requested-Auth: Digest', 'X-Opencast-Matterhorn-Authorization: true'])
        c.setopt(pycurl.WRITEFUNCTION, b.write)
        #c.setopt(pycurl.VERBOSE, True)
        c.perform()
        status_code = c.getinfo(pycurl.HTTP_CODE)

        if status_code == 200:
            # Create an XML document tree out of the response
            document = etree.fromstring(b.getvalue())

            # Modify curl settings to perform an ADD in the index instead
            c.setopt(pycurl.URL, urljoin(argv.search_url, argv.add_endpoint))
            c.setopt(pycurl.POST, 1)

            # For every mediapackage in the results...
            for mp in document.iter('{%s}mediapackage' % MP_NAMESPACE):
                modified=False
                # Search the URLs and parse them with urlparse...
                for url in mp.iter('{%s}url' % MP_NAMESPACE):
                    parsed = urlparse(url.text)
                    # Copy the urlparse into a list, so that it can be modified
                    aux = list(parsed)

                    # Ignore if the protocol is in the list of excluded one
                    if parsed.scheme in argv.excluded_protocols:
                        print "Excluding URL {}\n".format(url.text)
                        continue

                    # Check whether the URL protocol or server address are correct (i.e. match the download server)...
                    if parsed.scheme != dls.scheme or parsed.netloc != dls.netloc:
                        # Change the fields corresponding the server name and the protocol
                        aux[0] = dls.scheme
                        aux[1] = dls.netloc

                    # Check whether the download path is correct
                    if path.commonprefix( [ parsed.path, dls.path ] ) != dls.path:
                        # Extract the download server "mountpoint"
                        # Matterhorn resource URLs in distributed mediapackage take the form:
                        #    distribution-channel/mediapackage-id/element-id/filename.extension
                        # , therefore, anything that is beyond these four levels in the hierarchy 
                        # is a part of the download server "mountpoint"
                        for i in range(4):
                            aux[2] = path.dirname(aux[2])                        
                        # Remove the "mountpoint" from the resource's path and add the final "mountpoint" from the server
                        aux[2] = path.join(dls.path, path.relpath(parsed.path, aux[2]))

                    new_url = ParseResult(*aux)
                    if new_url != parsed:
                        url.text = new_url.geturl()
                        print "In:  {}\nOut: {}\n".format(parsed.geturl(), url.text)
                        if not modified:
                            modified = True
                    else:
                        print "URL {} NOT modified".format(url.text)

                # Overwrite the mediapackage in the index
                if modified:
                    # Upload the mediapackage back to the search index (overwriting the old version)
                    curl(etree.tostring(mp, encoding="UTF-8"), argv.search_url, argv.add_endpoint, argv.user, argv.password)
                else:
                    pass
                    
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


def lower_str(str):
    """
    Returns a lowercase string. This is to make the checks for the excluded protocols case insensitive
    """
    return str.lower()


if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Edit URLs in mediapackages published in Opencast, for instance when a download server URL changes.")

    parser.add_argument('search_url', help='URL of the machine running the Search service where the URLs will be updated')
    parser.add_argument(
        'new_dls',
        help='New URL to which the old ones will be converted to. IT HAS TO BE THE FULL URL, AS SPECIFIED IN THE MATTERHORN CONFIG. '
        'Example: http://pre-engage.rrz.uni-koeln.de:8080/static')
    parser.add_argument(
        'excluded_protocols', nargs='*', default=EXCLUDE_PROTO, type=lower_str,
        help='A list of space-separated URI protocols that shall not be modified. If none given, defaults to ''{0}'''.format(EXCLUDE_PROTO))
    parser.add_argument('-u', '--user', help='Digest user to access the Search service')
    parser.add_argument('-p', '--password', help='Digest password to access the Search service')
    parser.add_argument(
        '-s', '--search_endpoint', default=DEFAULT_SEARCH_ENDPOINT,
        help='Endpoint, relative to the server URL, that should return the list of published mediapackages. (Default: ''{0}'')'.format(DEFAULT_SEARCH_ENDPOINT))
    parser.add_argument(
        '-a', '--add_endpoint', default=DEFAULT_ADD_ENDPOINT,
        help='Endpoint, relative to the server URL, that should edit a published mediapackage. (Default: ''{0}'')'.format(DEFAULT_ADD_ENDPOINT))

    #print(parser.parse_args())
    #exit(0)


    sys.exit(main(parser.parse_args()))
