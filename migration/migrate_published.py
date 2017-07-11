#! /bin/python

"""Script to merge mediapackages that are published at a certain Opencast/Matterhorn system"""

from __future__ import print_function

from copy import deepcopy
import errno
import filecmp
import mimetypes
import os
import posixpath as urlpath
import shutil
import subprocess
import sys
import urlparse
from lxml import etree
from migrate_archived import get_src_and_dst_paths

import config
import utils


def get_relative_path(url_path):
    """
    Remove the server "mount point" from a URL path
    'url_path' should be a string containing a path extracted from a URL
    """
    # Check if the the path contains a "tag" (streaming URL)
    prefix, sep, suffix = url_path.partition(':')
    if sep == ':':
        # There is a "tag" in the URL. Separate the tag and whatever comes before it
        # This assumes that a tag comes immediately after a directory separator, e.g.
        # this/is/the/prefix/thetag:this/is/the/suffix
        prefix, tag = urlpath.split(prefix)

        # Reconstruct the URL removing the "tag" part
        clean_path = urlpath.join(prefix, suffix)

        # Check if this is a "smil" tag.
        # This is to support the Wowza adaptive streaming plugin
        if tag == config.smil_extension[1:]:
            # In this case the file name is "virtual".
            # The real file is the directory name before it
            clean_path = urlpath.dirname(clean_path)

        # Get this path's extension
        ext = urlpath.splitext(clean_path)[1]
        if ext == "":
            # Append the exception if it did not exist
            # This is to comply with some streaming URL formats
            # which omit the extension when it matches the tag
            ext = '.' + tag
            clean_path = clean_path + ext
        elif ext != '.' + tag:
            print(
                u"WARNING: Found conflicting tag in path '{0}'. \
                Ignoring the tag '{1}'".format(clean_path, tag), file=sys.stderr)
    else:
        # Normal URL, without streaming "tags"
        clean_path = url_path
        # Get the path extension
        ext = urlpath.splitext(clean_path)[1]

    if ext == "":
        print(u"WARNING: Found URL path without extension: {0}".format(url_path))

    # Extract the download server "mountpoint"
    # Matterhorn resource URLs in distributed mediapackage take the form:
    #    distribution-channel/mediapackage-id/element-id/filename.extension
    # , therefore, anything that is beyond these four levels in the hierarchy
    # is a part of the download server "mountpoint"
    # The exception to this rule are the Wowza SMIL files, which are directly
    # under the service root directory and therefore their paths do not need need
    # to be further processed
    root_path = urlpath.dirname(clean_path)
    if ext != config.smil_extension:
        for dummy in range(3):
            root_path = urlpath.dirname(root_path)

    # Remove the "mountpoint" from the resource's path to get the system path
    return os.path.normpath(urlpath.relpath(clean_path, root_path))


def get_source_path(rel_path):
    """
    Return the first existing path that results from combining one of the
    candidates specified in the configuration with the provided relative path
    """
    for parent in config.search_dirs:
        # Calculate the absolute path
        abs_path = os.path.join(parent, rel_path)
        if os.path.isfile(abs_path):
            # Assume the first match is the right one
            return abs_path

    raise utils.MissingElementException(
        "Could not find the path {0} among the configured candidates".format(rel_path))


def get_destination_path(rel_path, root):
    """
    Calculate the path where to copy an element depending on its extension
    """
    ext = os.path.splitext(rel_path)[1]

    if ext == config.smil_extension:
        # Append the relative path to the root without modification
        return os.path.join(root, rel_path)
    else:
        # Keep the just the two deeper levels (element ID and filename)
        reduced_path = rel_path
        for dummy in range(2):
            reduced_path = os.path.dirname(reduced_path)
        return os.path.join(root, os.path.relpath(rel_path, reduced_path))


def copy_element(url_path, root_dst_path, copied_paths):
    """
    Copies the element referenced by the XML object URL, which must be part of an XML
    representation of a MediaPackage element
    'root_dst_path' is the root path to copy the element referenced by the URL
    'copied_paths' is a list of all the paths already copied in this execution. It is used to
    detect duplicates in the mediapackage and distinguish them from files copied in previous
    execution of the script where a later failure prevented the ingestion to be completed.
    'smil_paths' is a list of all the SMIL files already copied. Because it is so likely that
    the same SMIL file is referenced more than once in the MP, we make sure it is not included
    here to avoid further processing.
    """

    url_path = get_relative_path(url_path)

    try:
        # Source path where to copy this element from
        src_path = get_source_path(url_path)

        # Destination path where to copy this element
        dst_path = get_destination_path(url_path, root_dst_path)
    except utils.MissingElementException:
        # Handle the case where archive URLs were published directly in the search index
        # This is a last resort measure
        src_path, dst_path = get_src_and_dst_paths(url_path, root_dst_path)

    if os.path.exists(dst_path) and dst_path in copied_paths:
        # The element was already copied!
        raise utils.DuplicateElementException(
            "Path '{0}' already processed with element '{1}'".format(
                dst_path,
                copied_paths[dst_path].get('id')))

    if not (os.path.exists(dst_path) and filecmp.cmp(src_path, dst_path)):
        # Either the file does not exist or it does, but it does not match the source file
        try:
            # Attempt to create the local directories
            os.makedirs(os.path.dirname(dst_path), config.dir_mode)
        except OSError as os_err:
            if os_err.errno == errno.EEXIST:
                # The directory already exists
                print(
                    u"WARN: Tried to create an already-existing directory: '{0}'"
                    .format(os.path.dirname(dst_path)), file=sys.stderr)
            else:
                # Raise the exception in any other case
                raise

        # Copy the file itself
        shutil.copyfile(src_path, dst_path)
    else:
        # The file was probably copied in a previous run of the script
        pass

    return dst_path


def copy_files_from_smil(smil_element, root_dst_path, copied_paths, quality_tags):
    """
    Process SMIL file to copy all the videos contained in the file under the
    'root_dst_path' directory. Generate the XML elements but does not add them to the tree.
    Instead, return them as a tuple
    'smil_element' is the mediapackage XML object referencing the
    SMIL file. We assume the element URL is relative to the 'root_dst_path'.
    'copied_paths' is a list of files, relative to 'root_dst_path', which have already been
    copied by this script. It is used to detect duplicates.
    'quality_tags' is a list of quality tags applied to the videos, sorted from lowest to
    highest. Because the 'video' tags in the SMIL file are assumed to be sorted also from the
    lowest to the highest quality, the track generated from the latest video in the SMIL file
    will receive the highest quality tag in the list, and so on.
    **ATTENTION**: If the number of qualities in the list does not match the number of videos
    in the SMIL file, the lower qualities will be discarded.
    """

    retval = []

    # Calculate path to the SMIL file
    smil_path = os.path.join(root_dst_path, smil_element.find(config.url_xml_tag).text)

    with open(smil_path, 'r+') as smil_file:
        for index, video_xml in enumerate(reversed(etree.parse(smil_file).findall('.//video'))):
            # Break if there are more videos in the SMIL file as qualities
            # Ignore if the list of quality tags is empty
            if quality_tags and index >= len(quality_tags):
                break

            # Create a new MP element
            new_xml = deepcopy(smil_element)

            # Make sure it's a track
            new_xml.tag = config.track_xml_tag

            # Delete forbidden attributes
            for att in [
                    att for att in new_xml.attrib if att in config.smil_filter_attributes]:
                del new_xml.attrib[att]

            # Delete quality tags
            tags_xml = new_xml.find(config.tags_xml_tag)
            for tag in [tag for tag in new_xml.iter(config.tag_xml_tag)
                        if tag.text in quality_tags]:
                print("[DEBUG] Removing quality tag from SMIL file '{}': '{}'"
                      .format(smil_path, tag.text))
                tags_xml.remove(tag)

            # Add quality tag, if the list is not empty
            if quality_tags:
                tag_xml = etree.SubElement(tags_xml, config.tag_xml_tag)
                tag_xml.text = quality_tags[index]

            # Copy element file
            dst_path = copy_element(
                video_xml.get(config.smil_src_attr),
                root_dst_path,
                copied_paths)

            # Add the path to the "copied paths" to avoid duplicates
            copied_paths[dst_path] = new_xml

            # Fix new element's URL
            new_xml.find(config.url_xml_tag).text = os.path.relpath(dst_path, root_dst_path)

            # Assuming an absolute path of:
            #     root/mp_id/element_id/filename
            # obtain the new element ID by removing the filename and the root
            new_xml.set(
                config.mp_elem_id_attr,
                os.path.basename(os.path.dirname(dst_path)))

            # Adjust mimetype, if present
            mimetype_xml = new_xml.find(config.mimetype_xml_tag)
            if mimetype_xml is not None:
                mimetype_xml.text = mimetypes.guess_type(dst_path)[0]

            # Add element to the return list
            retval.append(new_xml)

    return retval


def get_quality_tags(mp_xml, is_quality_tag=config.is_quality_tag):
    """
    Return a list of tags indicating qualities.
    Whether or not a tag represents a quality is determined by the function 'is_quality_tag',
    which accepts a string representing a tag and returns true if it is a quality tag.
    """
    retval = set()

    for tag in mp_xml.iter(config.tag_xml_tag):
        if is_quality_tag(tag.text):
            retval.add(tag.text)

    return list(sorted(retval, reverse=True))


def migrate_published(mp_id):
    """
    Exports the published mediapackage with the ID given as argument.
    The argument might be a simple ID or an xml tree representing the mediapackage
    """
    mp_xml = mp_id
    try:
        if isinstance(mp_id, basestring):
            # Try to find the mediapackage in the source system
            mp_xml = utils.get_unique_mp(
                mp_id,
                utils.get_url(config.src_engage, config.ep_search_list),
                config.src_auth)
        else:
            # In this case, the argument should be an XML tree. Rename arguments appropriately
            mp_id = mp_xml.get('id')

        if mp_xml.tag != config.mp_xml_tag:
            raise Exception(
                'Expected mediapackage XML representation. Got {0}'
                .format(mp_xml.tag))
    except AttributeError as attr_e:
        # mp_id/mp_xml did not behave like an XML object
        raise Exception('Expected XML tree. Got {0}: {1}'.format(type(mp_xml).__name__, attr_e))

    # Calculate the directory where to copy all the elements in the mediapackage
    series = mp_xml.find(config.series_xml_tag)
    if series is not None and series.text:
        mp_dir = os.path.join(config.search_copy_dir, series.text, mp_id)
    else:
        mp_dir = os.path.join(config.search_copy_dir, mp_id)

    # Make sure this MP was not already ingested
    ingested_file = os.path.join(mp_dir, config.ingested_filename)
    if os.path.isfile(ingested_file):
        raise utils.IngestedException(
            "Mediapackage '{0}' was already marked as ingested".format(mp_id))

    # Check the mediapackage has not already been exported
    try:
        utils.get_unique_mp(
            mp_id,
            utils.get_url(config.dst_engage, config.ep_search_list),
            config.dst_auth)
        raise utils.FoundException(
            ("Mediapackage '{0}' is not marked as ingested, " +
             "but is already published in the destination system").format(mp_id))
    except utils.NotFoundException:
        # This is expected
        pass

    # Get all available quality tags, if any
    quality_tags = get_quality_tags(mp_xml)

    # Keep track of duplicate paths or paths copied in a previous, failed run
    copied_paths = dict()

    # Keep track of duplicate SMIL paths. Those are likely to occur in a MP and
    # should not be considered an anomaly
    smil_paths = dict()

    # Iterate the mediapackage and copy its elements to the working directory
    for url in mp_xml.iter(config.url_xml_tag):
        # The element to which this URL belongs
        # We can do this, because in a well-formed mediapackage, URLs always have parents
        # (i.e. URLs always belong to MP elements)
        element = url.getparent()

        # Filter out elements based on their flavor
        if element.get(config.mp_flavor_attr) in config.filter_flavors:
            print(
                "[WARN] Removing element '{0}' because of its flavor: '{1}'".format(
                    element.get(config.mp_elem_id_attr),
                    element.get(config.mp_flavor_attr)),
                file=sys.stderr)
            # Delete element. We can invoke "getparent" twice, because in well-formed
            # mediapackages, all URLs are contained within elements, and all elements
            # are contained in categories ('media', 'metadata', etc.)
            element.getparent().remove(element)
            continue

        # Filter out elements based on their XML tag
        if element.tag in config.filter_tags:
            print(
                "[WARN] Removing element '{0}' because of its XML tag: '{1}'".format(
                    element.get(config.mp_elem_id_attr),
                    element.tag),
                file=sys.stderr)
            # Delete element. We can invoke "getparent" twice, because in well-formed
            # mediapackages, all URLs are contained within elements, and all elements
            # are contained in categories ('media', 'metadata', etc.)
            element.getparent().remove(element)
            continue

        try:
            dst_path = copy_element(
                urlparse.urlparse(url.text).path,
                mp_dir,
                copied_paths)

            # Mark the path as copied
            copied_paths[dst_path] = element

            # Fix the url in the XML element
            url.text = os.path.relpath(dst_path, mp_dir)
        except utils.DuplicateElementException as dee:
            # Log the situation
            print(
                "[WARN] Found duplicate path in mediapackage '{0}', element '{1}': {2}".format(
                    mp_id,
                    element.get(config.mp_elem_id_attr),
                    dee),
                file=sys.stderr)
            # Remove the duplicate element from the XML
            element.getparent().remove(element)
            continue

        if os.path.splitext(dst_path)[1] == config.smil_extension:
            if dst_path in smil_paths:
                # This is a duplicate of an already-processed SMIL file. Mark it as such
                smil_paths[dst_path][1].append(element)
            else:
                print("Processing SMIL file at {0}".format(dst_path))
                smil_paths[dst_path] = ([], [])

                # Process SMIL file, get newly generated elements and store them in the type
                smil_paths[dst_path][0].extend(
                    copy_files_from_smil(element, mp_dir, copied_paths, quality_tags))

                # Add the SMIL element to the list
                smil_paths[dst_path][1].append(element)

    # Process the SMIL dictionary
    for dst_path, (new_elements_list, smil_elements) in smil_paths.iteritems():
        # Delete SMIL elements
        for element in smil_elements:
            # We assume the SMIL files always have a parent
            element.getparent().remove(element)

        # Get the 'media' element in the mediapackage
        media_xml = mp_xml.find('.//' + config.media_xml_tag)

        # Add the track elements coming from the SMIL files
        for element in new_elements_list:
            media_xml.append(element)

        # Remove the SMIL file from the disk
        print("Borrando {0}: {1}".format(dst_path, os.remove(dst_path)))

    # Serialize the manifest
    with open(os.path.join(mp_dir, config.manifest_filename), "w+") as manifest_file:
        etree.ElementTree(mp_xml).write(
            manifest_file, encoding="utf-8", xml_declaration=True, pretty_print=True)

    # Zip the mediapackage
    zip_file = os.path.join(mp_dir, mp_id + '.zip')
    cwd = os.getcwd()
    os.chdir(mp_dir)
    subprocess.check_call(['zip', '-0ru', os.path.basename(zip_file)] + os.listdir('.'))
    os.chdir(cwd)

    # Copy the mediapackage in the inbox
    if os.path.isdir(config.search_inbox):
        shutil.copy(zip_file, config.search_inbox)
    else:
        raise OSError(errno.ENOENT, "The destination inbox does not exist", config.search_inbox)

    # Mark this MP as ingested
    # Simply create an empty file
    with open(ingested_file, 'w+'):
        pass

    print("Mediapackage {0} for SEARCH successfully ingested".format(mp_xml.get('id')))

    # Delete ingested files, if so configured
    if config.delete_ingested:
        for root, dirs, files in os.walk(mp_dir, topdown=False):
            for name in [fname for fname in files if fname != config.ingested_filename]:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))


if __name__ == '__main__':
    try:
        migrate_published(*sys.argv[1:])
        sys.exit(0)
    except Exception as exc:
        print("[ERROR]({0}) {1}".format(type(exc).__name__, exc), file=sys.stderr)
        sys.exit(1)
