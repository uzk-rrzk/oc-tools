#! /bin/python
# -*- coding:utf-8 -*-

""" Classes and Funtions to migrate an Opencast/Matterhorn system to another """

from abc import ABCMeta, abstractmethod
from copy import deepcopy
import logging
import logging.config
import mimetypes
import os
import posixpath as urlpath
from urlparse import urljoin, urlparse

from lxml import etree
import requests
from requests.auth import HTTPDigestAuth

# XML Namespace for mediapackages
XML_MP_NAMESP = "http://mediapackage.opencastproject.org"
# XML tag for the 'attachments' element in a mediapackage XML representation
XML_ATTACHS_TAG = '{{{0}}}attachments'.format(XML_MP_NAMESP)
# XML tag for the 'media' element in a mediapackage XML representation
XML_MEDIA_TAG = '{{{0}}}media'.format(XML_MP_NAMESP)
# XML tag for the 'metadata' element in a mediapackage XML representation
XML_METADATA_TAG = '{{{0}}}metadata'.format(XML_MP_NAMESP)
# XML tag for the 'mimetype' elements in a mediapackage XML representation
XML_MIME_TAG = '{{{0}}}mimetype'.format(XML_MP_NAMESP)
# XML tag of the root element of a mediapackage XML representation
XML_MP_TAG = '{{{0}}}mediapackage'.format(XML_MP_NAMESP)
# XML tag for the 'publications' element in a mediapackage XML representation
XML_PUBLS_TAG = '{{{0}}}publications'.format(XML_MP_NAMESP)
# XML tag of the 'series' element in a mediapackage XML representation
XML_SERIES_TAG = '{{{0}}}series'.format(XML_MP_NAMESP)
# XML tag for the 'tags' element in a mediapackage element XML representation
XML_TAGS_TAG = '{{{0}}}tags'.format(XML_MP_NAMESP)
# XML tag for the 'tag' elements in a mediapackage XML representation
XML_TAG_TAG = '{{{0}}}tag'.format(XML_MP_NAMESP)
# XML tag for the 'track' elements in a mediapackage XML representation
XML_TRACK_TAG = '{{{0}}}track'.format(XML_MP_NAMESP)
# XML tag of the 'url' elements in a mediapackage XML representation
XML_URL_TAG = '{{{0}}}url'.format(XML_MP_NAMESP)

# ACL XML namespace
XML_ACL_NAMESP = "http://org.opencastproject.security"
# ACL XML root tag
XML_ACL_ROOT_TAG = "{{{0}}}acl".format(XML_ACL_NAMESP)
# ACL XML element tag
XML_ACL_ELEMENT_TAG = "{{{0}}}ace".format(XML_ACL_NAMESP)
# ACL XML role tag
XML_ACL_ROLE_TAG = "{{{0}}}role".format(XML_ACL_NAMESP)
# ACL XML action tag
XML_ACL_ACTION_TAG = "{{{0}}}action".format(XML_ACL_NAMESP)
# ACL XML allow tag
XML_ACL_ALLOW_TAG = "{{{0}}}allow".format(XML_ACL_NAMESP)

# XML attribute representing a mediapackage (mediapackage element) identifier
XML_MP_ID_ATTR = 'id'
# XML attribute representing a mediapackage (mediapackage element) identifier
XML_MP_FLAVOR_ATTR = 'type'
# XML attribute representing a mediapackage track "transport" attribute
XML_MP_TRANSPORT_ATTR = 'transport'
# XML attribute representing a Wowza SMIL file's 'video-bitrate' attribute
XML_SMIL_BITRATE_ATTR = 'video-bitrate'
# XML attribute containing the 'source' element in the 'video' elements of a SMIL file
XML_SMIL_SRC_ATTR = 'src'

SMIL_EXT = '.smil'
SMIL_TAG = 'smil'
QUALITY_TAG_SUFFIX = '-quality'
# HTTP 'id' query
QUERY_ID = 'id'


class ServiceExport(object):
    """
    Export mediapackages from an Opencast/Matterhorn system
    """

    # Make this an abstract class
    __metaclass__ = ABCMeta

    ENDPOINT = None
    QUERY_SERIES = None
    QUERY_PAGE_SIZE = 'limit'
    QUERY_OFFSET = 'offset'
    DEFAULT_PAGE_SIZE = 50

    @property
    def mediapackage(self):
        """
        Return the migrated mediapackage as an XML document
        """
        return self._mp

    @property
    def mediapackage_id(self):
        """
        Return the ID of the current mediapackage
        """
        if self._mp is not None:
            return self._mp.get(XML_MP_ID_ATTR)
        return None

    @property
    def paths(self):
        """
        Return a dictionary with the asset paths to be copied.
        The keys of the dictionary are the relative paths of the assets in the MP,
        while the values are the absolute path where the asset can be found in the
        file system
        """
        return self._paths

    @abstractmethod
    def _get_paths(self, url_str):
        """
        For the provided mediapackage element URL, calculate:
            - the filesystem path where the element can be found (source)
            - the filesystem path where the element should be exported to (destination)
        and return both values as a tuple (source, destination)
        """
        pass

    # TODO Make this function, and not get_paths, abstract. Move get_paths to implementation
    def export_element(self, url):
        """
        Export a single element in the current mediapackage
        The argument is the URL subelement of the element to be migrated
        """
        # Get a reference to the element
        element = url.getparent()

        # Calculate source and destination paths of this element
        src_path, dst_path = self._get_paths(url.text)

        # TODO add configurable action when duplicate path found
        if dst_path in self._paths:
            # The path was already exported by another element!
            raise DuplicateElementException(
                "Element '{0}' duplicates path of already exported by element '{1}': {2}"
                .format(
                    element.get('id'),
                    self._exported[dst_path].get(XML_MP_ID_ATTR),
                    dst_path
                )
            )
        else:
            # Store the paths
            self._paths[dst_path] = src_path
            # Mark the element as exported
            self._exported[dst_path] = element
            # Fix the url in the XML element
            url.text = dst_path

    def reset(self):
        """
        Reset the internal state of this object
        """
        self._mp = None
        self._paths = {}
        self._exported = {}

    def export(self, mediap, filter_by_flavor=None, filter_by_tag=None, filter_tags=None):
        """
        Export a mediapackage from this service
        """

        # Initialize the mediapackage and path dict
        self.reset()

        if isinstance(mediap, basestring):
            # Get the mediapackage ID
            try:
                self._get_unique_mp(mediap)
            except MigrationException:
                self.reset()
                return
        else:
            self._mp = mediap

        if self._mp.tag != XML_MP_TAG:
            raise AttributeError(
                'Expected mediapackage XML representation. Got {0}'.format(XML_MP_TAG))

        # Delete publication elements (they cannot be ingested)
        # TODO Make it configurable
        publications = self._mp.find(XML_PUBLS_TAG)
        if publications is not None:
            publications.getparent().remove(publications)

        # Process its elements
        # Only the elements have 'url' children
        # Therefore, the parent of a 'url' tag is always an element
        for url in self._mp.iter(XML_URL_TAG):
            element = url.getparent()
            if filter_by_flavor and element.get(XML_MP_FLAVOR_ATTR) in filter_by_flavor:
                # Filter element because of its flavor
                element.getparent().remove(element)
                self._logger.debug(
                    "Filtering element '%s' because of its flavor: '%s",
                    element.get(XML_MP_ID_ATTR), element.get(XML_MP_FLAVOR_ATTR))
                continue
            if filter_by_tag or filter_tags:
                tags = element.find(XML_TAGS_TAG)
                if tags is not None:
                    removed = False
                    for tag in tags:
                        if filter_by_tag and tag.text in filter_by_tag:
                            # Filter element because of one of its tags
                            element.getparent().remove(element)
                            removed = True
                            self._logger.debug(
                                "Filtering element '%s' for having the tag: '%s",
                                element.get(XML_MP_ID_ATTR), tag.text)
                            break
                        if filter_tags and tag.text in filter_tags:
                            self._logger.debug(
                                "Filtering tag from element '%s': '%s'",
                                element.get(XML_MP_ID_ATTR), tag.text)
                            tags.remove(tag)
                    if removed:
                        continue

            # If none of the filters matched, process the mediapackage
            self.export_element(url)

    def get_mediapackages_from_series(self, series_id, offset=0, page_size=None):
        """
        Return a list of mediapackages from this services that belong
        to the provided series
        """
        if page_size is None:
            page_size = self.DEFAULT_PAGE_SIZE

        query = {
            self.QUERY_SERIES: series_id,
            self.QUERY_PAGE_SIZE: page_size,
            self.QUERY_OFFSET: offset
        }
        resp = requests.get(
            self._server,
            params=query,
            auth=self._auth
        )
        resp.raise_for_status()

        return etree.fromstring(resp.content).findall('.//'+XML_MP_TAG)

    def _get_unique_mp(self, mp_id):
        """
        Get an XML representation of the mediapackage and make sure
        that the result is unique.
        mp_id: id of the mediapackage to look for.

        Return: the mediapackage, as a XML document.
        """
        # Request the mediapackage list (of 1 element)
        query = {QUERY_ID: mp_id}
        resp = requests.get(self._server, params=query, auth=self._auth)
        resp.raise_for_status()

        mp_xml_list = etree.fromstring(resp.content).findall('.//' + XML_MP_TAG)

        if len(mp_xml_list) == 1:
            self._mp = mp_xml_list[0]
        elif len(mp_xml_list) == 0:
            raise NotFoundException(
                "Mediapackage '{0}' was NOT found at {1}".format(mp_id, resp.url))
        else:
            raise TooManyResultsException(
                "Search for mediapackage ID '{0}' at {1} returned {2} matches"
                .format(mp_id, resp.url, len(mp_xml_list)))


    def __init__(self, server, username, password, ignore_duplicates=False):
        self._server = urljoin(server, self.ENDPOINT)
        self._auth = OpencastDigestAuth(username, password)
        self._ignore_dups = ignore_duplicates

        self._mp = None
        self._paths = {}
        self._exported = {}

        self._logger = logging.getLogger(self.__class__.__name__)


class ArchiveServiceExport(ServiceExport):
    """ Export mediapackages from an Opencast/Matterhorn archive service """

    ENDPOINT = 'archive/episode.xml'
    LEGACY_ENDPOINT = 'episode/episode.xml'
    QUERY_SERIES = 'series'

    def __init__(self, server, username, password, archive_dir, legacy=False):
        """ Constructor """
        super(ArchiveServiceExport, self).__init__(server, username, password)
        self._arch_dir = archive_dir
        if legacy:
            self._server = urljoin(server, self.LEGACY_ENDPOINT)

    @property
    def archive_dir(self):
        """ The root directory where the archived elements will be exported from """
        return self._arch_dir

    def _get_paths(self, url_str):
        """
        For the provided mediapackage element URL, calculate:
            - the filesystem path where the element can be found (source)
            - the filesystem path where the element should be exported to (destination)
        and return both values as a tuple (source, destination)

        Paths in the URLs of archived mediapackages are of the form:

            episode/archive/mediapackage/{mediapackage_id}/{element_id}/{version}/{filename}.{ext}

        , where the filename is invented (meaning that it is used in the URLs, but does not
        correspond to the real name in the filesystem). The file extension '{ext}', however,
        matches the real extension of the file in the file system.

        The file structure within the archive directory in the file system is:

            {mediapackage_id}/{version}/{element_id}.{ext}

        , where {element_id}.{ext} is the real filename.

        Finally, we are aiming for a destination file structure like:

            element_id/filename.ext

        , so as to mimic the expected structure in an ingested mediapackage.
        """
        # Extract element's URL path
        url_path = urlparse(url_str).path

        # Extract the URL path components
        path, extension = urlpath.splitext(url_path)
        path, filename = urlpath.split(path)
        path, version = urlpath.split(path)
        path, element_id = urlpath.split(path)
        path, mp_id = urlpath.split(path)
        # Build the absolute source path
        src_path = os.path.normpath(
            os.path.join(self._arch_dir, mp_id, version, element_id + extension))

        # Make sure it exists
        if not os.path.isfile(src_path):
            raise MissingElementException(
                ("The path '{0}' corresponding to the URL path '{1}' "
                 "could not be found in the archive").format(src_path, url_path))

        # Return the source and destination paths
        return src_path, os.path.normpath(os.path.join(element_id, filename + extension))

    def export_element(self, url):
        """
        Export a single element in the current mediapackage
        The argument is the URL subelement of the element to be migrated

        This version ignores duplicate elements by simply removing them
        """
        try:
            super(ArchiveServiceExport, self).export_element(url)
        except DuplicateElementException as dee:
            # TODO We simply ignore duplicates and go on, but it's dangerous
            self._logger.warn(dee)
            # Remove duplicate element
            element = url.getparent()
            element.getparent().remove(element)


# Because we have found archive paths among the published elements, we had to make
# this class inherit from ArchiveServiceExport, so that we can optionally use its
# path resolution mechanism
class PublishServiceExport(ArchiveServiceExport):
    """ Export mediapackages from an Opencast/Matterhorn 'search' service """

    ENDPOINT = 'search/episode.xml'
    QUERY_SERIES = 'sid'

    def __init__(self, server, username, password, search_dirs, archive_dir=''):
        """ Constructor """
        super(PublishServiceExport, self).__init__(server, username, password, archive_dir)
        self.__search_dirs = set()
        if isinstance(search_dirs, basestring):
            self.__search_dirs.update([search_dirs])
        elif search_dirs:
            self.__search_dirs.update(search_dirs)

        self.__quality_tags = []
        self.__elements_from_smil = []

    def _get_unique_mp(self, mp_id):
        """
        Get an XML representation of the mediapackage and make sure
        that the result is unique.
        mp_id: id of the mediapackage to look for.

        Return: the mediapackage, as a XML document.
        """
        super(PublishServiceExport, self)._get_unique_mp(mp_id)

        # Process quality tags
        self.__extract_quality_tags()

    def reset(self):
        super(PublishServiceExport, self).reset()
        self.__quality_tags = []
        self.__elements_from_smil = []

    def _get_clean_path(self, url):
        """
        Remove the server "mount point" from an element's URL and 'clean' the path,
        removing any stream specifiers and converting it into a regular filesystem
        path
        """
        parsed = urlparse(url)
        path = parsed.path

        # If the url is relative and starts with a stream specifier (a 'tag'), the urlparse
        # method will incorrectly assume that such 'tag' is in reality the URL's scheme
        # We can detect that situation because, in the URLs generated by Opencast,  the 'netloc'
        # component (i.e. the server name, port, etc) is never empty when the URL also has a scheme
        tag = None
        if parsed.scheme and not parsed.netloc:
            # The 'scheme' is in reality the stream specifier (the 'tag')
            # In this case, the original URL path needs no further processing
            tag = parsed.scheme
        else:
            # Check if the the path contains a "tag" (stream specification)
            prefix, sep, suffix = parsed.path.partition(':')
            if sep == ':':
                # There is a "tag" in the URL. Separate the tag and whatever comes before it
                # This assumes that a tag comes immediately after a directory separator, e.g.
                # this/is/the/prefix/thetag:this/is/the/suffix
                prefix, tag = urlpath.split(prefix)

                # Reconstruct the URL removing the "tag" part
                path = urlpath.join(prefix, suffix)

        # Check if the URL has a "smil" tag.
        # This is to support the Wowza adaptive streaming plugin
        if tag == SMIL_TAG:
            # In this case the file name is "virtual".
            # The real file is the directory name before it
            path = urlpath.dirname(path)

        # Get this path's extension
        ext = urlpath.splitext(path)[1]
        if not ext:
            # The path has no extension
            if tag:
                # Append the exception if it did not exist
                # This is to comply with some streaming URL formats
                # which omit the extension when it matches the tag
                ext = '.' + tag
                path += ext
            else:
                # The URL had neither tag nor extension
                self._logger.warn(
                    "Parsed URL path for '%s' has no extension: '%s'", parsed.path, path)
        elif tag and ext != '.' + tag:
            # The URL has both a tag and a extension, but they do not match
            self._logger.warn(
                "Found conflicting tag in path '%s'. Ignoring the tag '%s'", path, tag)
        # Extract the server "mountpoint"
        # Matterhorn resource URLs in distributed mediapackage take the form:
        #    distribution-channel/mediapackage-id/element-id/filename.extension
        # , therefore, anything that is beyond these four levels in the hierarchy
        # is a part of the download server "mountpoint"
        # The exception to this rule are the Wowza SMIL files, which are directly
        # under the service root directory and therefore their paths do not need need
        # to be further processed
        root_path = urlpath.dirname(path)
        if ext != SMIL_EXT:
            for dummy in range(3):
                root_path = urlpath.dirname(root_path)
        # Remove the "mountpoint" from the resource's path to get the system path
        return os.path.normpath(urlpath.relpath(path, root_path))

    @staticmethod
    def is_quality_tag(tag):
        """
        Whether the provided string can be a mediapackage element tag
        indicating a certain video quality
        """
        return tag.endswith(QUALITY_TAG_SUFFIX)

    def __extract_quality_tags(self):
        """
        Extract a list of tags indicating qualities found in the provided mediapackage.
        The list is sorted from higher to lower quality.
        Whether or not a tag represents a quality is determined by the function 'is_quality_tag',
        which accepts a string representing a tag and returns true if it is a quality tag.
        """
        tags = set()
        for tag in self._mp.iter(XML_TAG_TAG):
            if self.is_quality_tag(tag.text):
                tags.add(tag.text)
        self.__quality_tags = list(sorted(tags, reverse=True))

    def __get_src_path(self, rel_path):
        """
        Return the first existing path that results from combining one of the
        candidates specified in the configuration with the provided, clean URL path
        """
        for parent in self.__search_dirs:
            # Calculate the absolute path
            abs_path = os.path.join(parent, rel_path)
            if os.path.isfile(abs_path):
                # Assume the first match is the right one
                return abs_path

        raise MissingElementException(
            "Could not find the path {0} among the configured candidates".format(rel_path))

    @staticmethod
    def __get_dst_path(rel_path):
        """
        Return the path where an element should be exported depending on its extension
        The argument must be a clean URL path
        """
        ext = os.path.splitext(rel_path)[1]

        if ext == SMIL_EXT:
            # Return the relative path without modification
            return rel_path
        else:
            # Keep the just the two deeper levels (element ID and filename)
            reduced_path = rel_path
            for dummy in range(2):
                reduced_path = os.path.dirname(reduced_path)
            return os.path.relpath(rel_path, reduced_path)

    def _get_paths(self, url_str):
        """
        For the provided mediapackage element URL, calculate:
            - the filesystem path where the element can be found (source)
            - the filesystem path where the element should be exported to (destination)
        and return both values as a tuple (source, destination)
        """
        # Extract element's "cleaned", relative path
        # This is the element's URL path after removing the server mount point and any
        # streaming "format" tags that may exist
        clean_path = self._get_clean_path(url_str)

        try:
            return self.__get_src_path(clean_path), self.__get_dst_path(clean_path)
        except MissingElementException:
            # Handle the rare case where a path from the archive
            # has been published without modification
            if self._arch_dir:
                return super(PublishServiceExport, self)._get_paths(clean_path)
            raise

    def export_element(self, url):
        """
        Export a single element in the current mediapackage
        The argument is the URL subelement of the element to be migrated

        This version ignores duplicates by removing them without further action
        It also processes any SMIL files present in the mediapackage and
        instantiates new elements to represent the tracks in the SMIL files.
        """
        # Reference to the element
        element = url.getparent()
        # Get the clean path for later
        clean_path = self._get_clean_path(url.text)
        try:
            # We call the "grandfather" explicitly, because we do not want
            # to ignore DuplicateElementException's at this point, and
            # ArchiveServiceExport does
            ServiceExport.export_element(self, url)
            # Check whether or not this is a SMIL file
            if os.path.splitext(clean_path)[1] == SMIL_EXT:
                # Get new elements from the contents of this SMIL file
                self.__export_from_smil(element, self.__get_src_path(clean_path))
                # Delete this element
                element.getparent().remove(element)
        except DuplicateElementException as dee:
            # TODO We simply ignore duplicates and go on, but it's dangerous
            if os.path.splitext(clean_path)[1] == SMIL_EXT:
                self._logger.debug("Found duplicate SMIL file: %s", dee)
            else:
                self._logger.warn(dee)
            # Remove duplicate element
            element.getparent().remove(element)

    def export(self, mp, filter_by_flavor=None, filter_by_tag=None, filter_tags=None):
        super(PublishServiceExport, self).export(
            mp, filter_by_flavor, filter_by_tag, filter_tags)

        # Add files extracted from SMIL
        # They are all tracks, so we add them to the "media" section
        if self._mp is not None:
            self._mp.find(XML_MEDIA_TAG).extend(self.__elements_from_smil)

    def __export_from_smil(self, smil_element, smil_path):
        """
        Process SMIL file to copy all the videos contained in the file under the
        'root_dst_path' directory. Generate the XML elements but do not add them to the tree.
        'smil_element' is the mediapackage element referencing the SMIL file. It is used as a
        model from which the new elements will be created.
        'smil_path' is the path where the SMIL file is. This path must be accessible for reading.
        **ATTENTION**: This method takes into account the '__quality_tags' parameter of the class,
        which contains a list of the different element tags representing a video quality.
        If the number of qualities in the list does not match the number of videos
        in the SMIL file, the lower qualities will be discarded. Similarly, if there are more
        videos in the SMIL file than qualities in the list, those with the lowest bitrate will
        not be exported.
        """

        # Modify the element's attributes, to use it as a model to clone all the new elements from
        # Make sure it's a track
        smil_element.tag = XML_TRACK_TAG
        # * Delete the 'transport' attribute
        try:
            del smil_element.attrib[XML_MP_TRANSPORT_ATTR]
        except KeyError:
            # The element did not contain the attribute already. This is fine
            pass
        # * Delete existing quality tags
        tags_xml = smil_element.find(XML_TAGS_TAG)
        if tags_xml is not None:
            for tag in tags_xml.iterfind(XML_TAG_TAG):
                if tag.text in self.__quality_tags:
                    self._logger.debug(
                        "Removing quality tag from SMIL file '%s': '%s'", smil_path, tag.text)
                    tags_xml.remove(tag)

        # Process the SMIL file
        with open(smil_path, 'r') as smil_file:
            self._logger.debug("Opened smil file: '%s'", smil_path)
            # Get all the videos in the SMIL file and sort them by bitrate, descending
            videos = sorted(
                etree.parse(smil_file).findall('.//video'),
                key=lambda video: int(video.get(XML_SMIL_BITRATE_ATTR)),
                reverse=True
            )

            tag_index = 0
            for video_xml in videos:
                # Break if there are more videos in the SMIL file as qualities
                # Ignore if the list of quality tags is empty
                if self.__quality_tags and tag_index >= len(self.__quality_tags):
                    break

                # Create a new MP element.
                new_element = deepcopy(smil_element)
                new_element_url = new_element.find(XML_URL_TAG)

                # Assign the corresponding URL
                new_element_url.text = video_xml.get(XML_SMIL_SRC_ATTR)

                try:
                    # Export this element
                    # Use the 'grandparent' method, because the DuplicateElementException
                    # is ignored in both this class' and this class' parent's method
                    # If that exception is raised, the element is no further processed
                    ServiceExport.export_element(self, new_element_url)

                    # Assuming a exported URL (a filesystem path, actually) of:
                    #     root/mp_id/element_id/filename
                    # obtain the new element ID by removing the filename and the root
                    new_element.set(
                        XML_MP_ID_ATTR,
                        os.path.basename(os.path.dirname(new_element_url.text))
                    )

                    # Adjust mimetype, if present
                    mimetype_xml = new_element.find(XML_MIME_TAG)
                    if mimetype_xml is not None:
                        mimetype_xml.text = mimetypes.guess_type(new_element_url.text)[0]

                    # Get this element's tags
                    tags_xml = new_element.find(XML_TAGS_TAG)
                    if tags_xml is None:
                        # Create the 'tags' subelement, if it does not exist
                        tags_xml = etree.SubElement(new_element, XML_TAGS_TAG)
                    # Add a quality tag, if the list is not empty
                    if self.__quality_tags:
                        tag_xml = etree.SubElement(tags_xml, XML_TAG_TAG)
                        tag_xml.text = self.__quality_tags[tag_index]
                        self._logger.debug(
                            "Added quality tag %s to element %s",
                            self.__quality_tags[tag_index], new_element.get(XML_MP_ID_ATTR)
                        )
                        tag_index += 1

                    # Add element to the return list
                    self.__elements_from_smil.append(new_element)
                except DuplicateElementException as dee:
                    self._logger.warn(
                        "Ignoring duplicate element found in SMIL file '%s': %s", smil_path, dee)


class Export(object):
    """
    Consolidate mediapackages from several Opencast/Matterhorn services
    and export them to a certain folder on disk
    """
    def __init__(self, *services):
        self._mp = None
        self._paths = None
        self._main = None
        self._slaves = []

        for service in services:
            self.register(service)

        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def mediapackage(self):
        """ Return this object's current mediapackage as an XML tree """
        return self._mp

    @property
    def paths(self):
        """ Return this object's current list of paths to migrate """
        return self._paths


    #The available categories are:
    #    - '{http://mediapackage.opencastproject.org}media', for tracks
    #    - '{http://mediapackage.opencastproject.org}metadata', for catalogs
    #    - '{http://mediapackage.opencastproject.org}attachments', and
    #    - '{http://mediapackage.opencastproject.org}publications'
    # Please note that the exiting 'ServiceMigration' implementations filter out all the
    # publications, so the 'publications' category is normally useless.
    # Use the constants defined in the package, instead of the string literals
    # This list indicates this class should migrate tracks and attachments, ignoring the
    # metadata
    MERGE_CATEGORIES = (XML_MEDIA_TAG, XML_ATTACHS_TAG)

    def register(self, service):
        """
        Register a ServiceExport object
        """
        if service not in [self._main] + self._slaves:
            if isinstance(service, ServiceExport):
                if self._main is None:
                    self._main = service
                else:
                    self._slaves.append(service)
            else:
                raise TypeError(
                    "Error registering service. Expected 'ServiceExport'. Got: '{1}'".format(
                        type(service).__name__
                    )
                )
        else:
            raise DuplicateElementException('The service was already registered')

    def _merge_mp(self, mp_to_merge):
        """
        Merge the provided MP with the current one
        """
        try:
            mp_copy = deepcopy(mp_to_merge)
            for category in self.MERGE_CATEGORIES:
                dst = self._mp.find(category)
                src = mp_copy.find(category)
                for child in src:
                    dst.append(child)
        except AttributeError:
            pass

    def _merge_paths(self, paths_to_merge):
        try:
            intersect = {dst: src for dst, src in paths_to_merge.iteritems()
                         if dst in self._paths and src != self._paths[dst]}
            for dst, src in intersect.iteritems():
                # TODO Do something more intelligent than just ignoring
                self._logger.warn(
                    "Found duplicate destination path '%s' with conflicting source paths: "
                    "'%s' and '%s'. Ignoring the second one...", dst, self._paths[dst], src)
            self._paths.update(
                {key: value for key, value in paths_to_merge.iteritems()
                 if key not in intersect}
            )
        except (AttributeError, TypeError) as err:
            self._logger.warn("Could not merge paths with %s: %s", paths_to_merge, err)

    def export(self, mediap, filter_by_flavor=None, filter_by_tag=None, filter_tags=None):
        """
        Export a mediapackage with the given ID from each registered service, merge the
        results and copy them to disk
        """

        # Initialize variables
        self._mp = None
        self._paths = {}

        if self._main is None:
            return

        self._main.export(mediap, filter_by_flavor, filter_by_tag, filter_tags)
        self._mp = self._main.mediapackage
        self._paths.update(self._main.paths)

        for service in self._slaves:
            service.export(
                self._main.mediapackage_id,
                filter_by_flavor,
                filter_by_tag,
                filter_tags)

            if service.mediapackage is not None:
                if self._mp is None:
                    self._mp = service.mediapackage
                    self._paths.update(service.paths)
                else:
                    self._merge_mp(service.mediapackage)
                    self._merge_paths(service.paths)

    def get_mediapackages_from_series(self, series_id, offset=0, page_size=None):
        """
        Get series list of the first registered service
        Return None if no services was yet registered
        """
        try:
            return self._main.get_mediapackages_from_series(
                series_id, offset, page_size)
        except AttributeError:
            return None


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
    query = {XML_MP_ID_ATTR: mp_id}
    resp = requests.get(
        url,
        params=query,
        auth=auth
    )
    resp.raise_for_status()

    mp_xml_list = etree.fromstring(
        resp.content).findall('.//' + XML_MP_TAG)

    if len(mp_xml_list) == 1:
        return mp_xml_list[0]
    elif len(mp_xml_list) == 0:
        raise NotFoundException(
            "Mediapackage '{0}' was NOT found at {1}".format(mp_id, resp.url))
    else:
        raise TooManyResultsException(
            "Search for mediapackage ID '{0}' at {1} returned {2} matches"
            .format(mp_id, resp.url, len(mp_xml_list)))


class MigrationException(Exception):
    """
    Base class for the custom exceptions defined here
    """
    pass

class FoundException(MigrationException):
    """
    Indicate that a mediapackage unexpectedly exists
    """
    pass

class NotFoundException(MigrationException):
    """
    Indicate that a mediapackage unexpectedly does not exist
    """
    pass

class TooManyResultsException(MigrationException):
    """
    Indicate when we get more than one result in a search, but only one was expected
    """
    pass

class IngestedException(MigrationException):
    """
    Indicate that a mediapackage or series is already marked as ingested
    """
    pass

class AlreadyFailedException(MigrationException):
    """
    In a mediapackage, indicate that an ingestion was already attempted and failed.
    In a series, indicate that some of its mediapackages failed to ingest.
    """
    pass

class MissingElementException(MigrationException):
    """
    Indicate when an element does not exist in the path it is supposed to
    """
    pass

class DuplicateElementException(MigrationException):
    """
    Indicate when two or more elements with the same URL are detected
    """
    pass
