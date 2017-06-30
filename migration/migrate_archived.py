#! /bin/python

"""Script to merge mediapackages that are archived at a certain Opencast/Matterhorn system"""

from __future__ import print_function

import errno
import filecmp
import os
import posixpath as urlpath
import shutil
import subprocess
import sys
import urlparse
from lxml import etree

import config
import utils


def get_src_and_dst_paths(url_path, dest_root):
    """
    Calculate the filesystem path for an archived element, based on its URL; also, calculate
    the path where the element should be copied by this script (under dest_root) and return
    both values as a tuple.

    Paths in the URLs of archived mediapackages are of the form:

        episode/archive/mediapackage/{mediapackage_id}/{element_id}/{version}/{filename}.{ext}

    , where the filename is invented (meaning that it is used in the URLs, but does not
    correspond to the real name in the filesystem). The file extension '{ext}', however,
    matches the real extension of the file in the file system.

    The file structure within the archive directory in the file system is:

        {mediapackage_id}/{version}/{element_id}.{ext}

    , where {element_id}.{ext} is the real filename.

    Finally, we are aiming for a destination file structure like:

        dest_root/element_id/filename.ext

    , so as to mimic the expected structure in an ingested file.
    """

    # Extract the URL path components
    path, extension = urlpath.splitext(url_path)
    path, filename = urlpath.split(path)
    path, version = urlpath.split(path)
    path, element_id = urlpath.split(path)
    path, mp_id = urlpath.split(path)

    # Build the absolute source path
    src_path = os.path.normpath(
        os.path.join(config.archive_dir, mp_id, version, element_id + extension))

    # Make sure it exists
    if not os.path.isfile(src_path):
        raise utils.MissingElementException(
            "The path {0} corresponding to the URL path {1} could not be found in the archive"
            .format(src_path, url_path))

    # Return the source and destination paths
    return src_path, os.path.normpath(os.path.join(dest_root, element_id, filename + extension))


def copy_element_and_fix_url(url, root_dst_path, copied_paths):
    """
    Copies the element referenced by the XML object URL, which must be part of an XML
    representation of a MediaPackage element
    'root_dst_path' is the root path to copy the element referenced by the URL
    'copied_paths' is a list of all the paths already copied in this execution. It is used to
    detect duplicates in the mediapackage and distinguish them from files copied in previous
    execution of the script where a later failure prevented the ingestion to be completed.
    """
    # Source path where to copy this element from
    src_path, dst_path = get_src_and_dst_paths(urlparse.urlparse(url.text).path, root_dst_path)

    if os.path.exists(dst_path) and dst_path in copied_paths:
        # The element was already copied!
        raise utils.DuplicateElementException(
            "Duplicate path in elements \
            {0} and {1}: {2}".format(
                copied_paths[dst_path].get('id'),
                url.getparent().get('id'),
                dst_path))

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

    # Mark the path as copied
    copied_paths[dst_path] = url.getparent()
    # Fix the url in the XML element
    url.text = os.path.relpath(dst_path, root_dst_path)

    return dst_path


def migrate_archived(mp_id):
    """
    Exports the archived mediapackage with the ID given as argument.
    The argument might be a simple ID or an xml tree representing the mediapackage
    """
    mp_xml = mp_id
    try:
        if isinstance(mp_id, basestring):
            # Try to find the mediapackage in the source system
            mp_xml = utils.get_unique_mp(
                mp_id,
                utils.get_url(config.src_admin, config.ep_src_archive_list),
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
        mp_dir = os.path.join(config.archive_copy_dir, series.text, mp_id)
    else:
        mp_dir = os.path.join(config.archive_copy_dir, mp_id)

    # Make sure this MP was not already ingested
    ingested_file = os.path.join(mp_dir, config.ingested_filename)
    if os.path.isfile(ingested_file):
        raise utils.IngestedException(
            "Mediapackage {0} was already marked as ingested".format(mp_id))

    # Check the mediapackage has not already been exported
    try:
        utils.get_unique_mp(
            mp_id,
            utils.get_url(config.dst_admin, config.ep_dst_archive_list),
            config.dst_auth)
        raise utils.FoundException(
            ("Mediapackage '{0}' is not marked as ingested, " +
             "but is already archived in the destination system").format(mp_id))
    except utils.NotFoundException:
        # This is expected
        pass

    # Keep track of duplicate paths or paths copied in a previous, failed run
    copied_paths = dict()

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
            copy_element_and_fix_url(url, mp_dir, copied_paths)
        except utils.DuplicateElementException as dee:
            # Log the situation
            print("[WARN] {0}".format(dee), file=sys.stderr)
            # Remove the duplicate element from the XML
            element.getparent().remove(element)
            continue

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
    if os.path.isdir(config.archive_inbox):
        shutil.copy(zip_file, config.archive_inbox)
    else:
        raise OSError(errno.ENOENT, "The destination inbox does not exist", config.archive_inbox)

    # Mark this MP as ingested
    # Simply create an empty file
    with open(ingested_file, 'w+'):
        pass

    print("Mediapackage {0} for ARCHIVE successfully ingested".format(mp_xml.get('id')))

    # Delete ingested files, if so configured
    if config.delete_ingested:
        for root, dirs, files in os.walk(mp_dir, topdown=False):
            for name in [fname for fname in files if fname != config.ingested_filename]:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))


if __name__ == '__main__':
    try:
        migrate_archived(*sys.argv[1:])
        sys.exit(0)
    except Exception as exc:
        print("[ERROR]({0}) {1}".format(type(exc).__name__, exc), file=sys.stderr)
        sys.exit(1)
