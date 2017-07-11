#! /bin/python

import requests
from lxml import etree
from requests.auth import HTTPDigestAuth
from urlparse import urljoin
import config


class OpencastDigestAuth(HTTPDigestAuth):
    """
    Implement a digest authentication including the headers required by Opencast
    """

    def __call__(self, r):
        # Call the parent method
        r = super(OpencastDigestAuth, self).__call__(r)

        # Add Opencast required headers
        r.headers['X-Requested-Auth'] = 'Digest'
        r.headers['X-Opencast-Matterhorn-Authorization'] = 'true'

        return r


def get_url(server, path, **params):
    """ Construct a URL with the provided arguments """
    return urljoin(server, path.format(**params))


def get_unique_mp(mp_id, url, auth):
    """
    Get an XML of the mediapackage with the ID 'id' at 'url'.
    Make sure the result is unique
    """

    # Request the mediapackage list
    query = {config.query_id: mp_id}
    resp = requests.get(
        url,
        params=query,
        auth=auth
    )
    resp.raise_for_status()

    mp_xml_list = etree.fromstring(
        resp.content).findall('.//' + config.mp_xml_tag)

    if len(mp_xml_list) == 1:
        return mp_xml_list[0]
    elif len(mp_xml_list) == 0:
        raise NotFoundException(
            "Mediapackage '{0}' was NOT found at {1}".format(mp_id, resp.url))
    else:
        raise TooManyResultsException(
            "Search for mediapackage ID '{0}' at {1} returned {2} matches"
            .format(mp_id, resp.url, len(mp_xml_list)))


class FoundException(Exception):
    """
    Indicate that a mediapackage unexpectedly exists
    """
    pass

class NotFoundException(Exception):
    """
    Indicate that a mediapackage unexpectedly does not exist
    """
    pass

class TooManyResultsException(Exception):
    """
    Indicate when we get more than one result in a search, but only one was expected
    """
    pass

class IngestedException(Exception):
    """
    Indicate that a mediapackage or series is already marked as ingested
    """
    pass

class AlreadyFailedException(Exception):
    """
    In a mediapackage, indicate that an ingestion was already attempted and failed.
    In a series, indicate that some of its mediapackages failed to ingest.
    """
    pass

class MissingElementException(Exception):
    """
    Indicate when an element does not exist in the path it is supposed to
    """
    pass

class DuplicateElementException(Exception):
    """
    Indicate when two or more elements with the same URL are detected
    """
    pass
