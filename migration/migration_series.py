#! /bin/python
"""
Utilities to migrate all the episodes in an Opencast/Matterhorn series into another system
"""
from collections import OrderedDict

import argparse
import errno
import filecmp
import logging
import logging.config
import os
import shutil
import subprocess
import sys

from lxml import etree
import requests
from requests.exceptions import HTTPError

import config

import migration

# Configure logging
logging.config.dictConfig(config.log_conf)
LOGGER = logging.getLogger()

def create_series(series_xml, series_acl, server, auth):
    """ Creates a new series with the given ACL """
    post_data = {
        config.ep_series_post_series: series_xml,
        config.ep_series_post_acl: series_acl
    }
    resp = requests.post(
        migration.get_url(server, config.ep_series_post),
        data=post_data,
        auth=auth)
    resp.raise_for_status()
    return resp.content

def get_series(series_id, server, auth):
    """ Reads the series with ID 'series_id' in XML format from the server """
    resp = requests.get(
        migration.get_url(server, config.ep_series_get, sid=series_id),
        auth=auth
    )
    resp.raise_for_status()
    return resp.content


def get_series_acl(series_id, server, auth):
    """ Reads the ACL of the series with ID 'series_id' in XML format """
    resp = requests.get(
        migration.get_url(server, config.ep_series_acl, sid=series_id),
        auth=auth
    )
    resp.raise_for_status()
    return resp.content


def edit_acl(acl, default_roles=None, transform_roles=None):
    """
    Edit a series ACL provided as an XML string, returning the modified ACL in the same format.

    If 'transform_roles' is not None, it should be a function, accepting a role as a string
    argument and a dictionary with the actions for that role. The format is just like that of
    the values for the 'default_roles' dictionary below. It must return a tuple (role, actions),
    with the (possibly modified) role name and actions to be added to the ACL.
    A return value of 'None', or a returned role name of 'None', will delete the role from the ACL
    entirely.

    If 'default_roles' is not None, the roles defined here will be added to the ACL
    **ONLY IF THEY ARE NOT PRESENT THERE YET**.
    'default_roles' MUST be a dictionary where:
        - Keys are strings representing the default roles.
        - Values are also dictionaries describing the permitted actions for this role, where:
            - Keys are strings representing actions ("read", "write", etc).
                  * Please note that Opencast handles the actions "read", "write" and "analyze"
                    only, but if the "analyze" action is found, its value is ignored and a
                    warn/debug message is logged. Whether or not other values will be ignored or
                    cause an error is undefined.
            - Values are either the string 'true' or the string 'false', indicating whether or not
              the role is allowed to perform the action.
    Please note that this script makes no attempt to convert the provided values to strings. Using
    any other data types in the 'default_roles' dictionary will likely rise a TypeError
    The roles in 'default_roles' are never converted using the 'transform_roles' function.
    """

    # Process the received acl
    acl_xml = etree.fromstring(acl)

    # Process the roles as as a dictionary tree:
    # - 1st level: dictionary with roles as keys and values a dict of the second level
    # - 2nd level: dictionary with actions as keys and actions as value
    acl_dict = acl_parse(acl_xml)

    # Generate new ACL
    new_acl = etree.Element(
        migration.XML_ACL_ROOT_TAG, nsmap={None: migration.XML_ACL_NAMESP})
    for role, actions in acl_dict.iteritems():
        try:
            if transform_roles:
                # Apply role transformation
                acl_add_elements(new_acl, *transform_roles(role, actions))
            else:
                # Use the role as-is
                acl_add_elements(new_acl, role, actions)
        except TypeError as type_err:
            LOGGER.error("Setting ACL for role '%s' found an illegal type: %s",
                         role, type_err)

    # Append default values
    try:
        for role, actions in default_roles.iteritems():
            if role not in acl_dict:
                try:
                    acl_add_elements(new_acl, role, actions)
                except TypeError as type_err:
                    LOGGER.error("Setting ACL for default role '%s'"
                                 "found an illegal type: %s", role, type_err)
    except AttributeError as attr_err:
        # default_roles is not a dict. Complain only if it is not None (it's the default)
        if default_roles is not None:
            LOGGER.warn("Could not set default ACL roles: %s", attr_err)

    return etree.tostring(new_acl, encoding='utf-8', xml_declaration=True, pretty_print=True)


def acl_parse(acl_xml):
    """
    Parse an XML tree representing an ACL and return a dictionary with the values.
    """

    # Process the roles as as a dictionary tree:
    # - 1st level: dictionary with roles as keys and values a dict of the second level
    # - 2nd level: dictionary with actions as keys and actions as value
    acl_dict = OrderedDict()
    for ace in acl_xml.iterfind(migration.XML_ACL_ELEMENT_TAG):
        role = ace.find(migration.XML_ACL_ROLE_TAG).text
        action = ace.find(migration.XML_ACL_ACTION_TAG).text
        allow = ace.find(migration.XML_ACL_ALLOW_TAG).text

        try:
            # Put the action in the dictionary
            acl_dict[role][action] = allow
        except KeyError:
            # There was not a value for the role. Create a dictionary with it
            acl_dict[role] = OrderedDict([(action, allow)])

    return acl_dict


def acl_add_elements(acl_xml, role, actions):
    """
    Add new roles for the role 'role' to the provided XML tree representing an ACL
    The "actions" argument should be a dictionary where the key are the "action" names
    and the values, their corresponding "allow" values (true or false)
    The roles are not added if the "role" provided is None

    This method may raise a TypeError, as it uses acl_add_element internally
    """
    if role is not None:
        try:
            for action, allow in actions.iteritems():
                # Opencast does not support deny ACLs, i.e. those where "allow" is false
                # Even though these are ignored, they cause a WARNING to be logged
                if allow.lower() == 'true':
                    acl_add_element(acl_xml, role, action, allow)
                else:
                    LOGGER.warn("Ignoring non-allow rule for role '%s' and action '%s': '%s'",
                                role, action, allow)
        except AttributeError as attr_err:
            # 'actions' is not a dict
            LOGGER.error("Could not set access control element for role '%s': %s", role, attr_err)


def acl_add_element(acl_xml, role, action, allow):
    """
    Add a new rule to the provided XML tree representing an ACL.
    The element is NOT added if 'role' is None
    This method may raise a TypeError if role, action or allow are not strings
    """

    if role is not None:
        ace = etree.SubElement(acl_xml, migration.XML_ACL_ELEMENT_TAG)
        etree.SubElement(ace, migration.XML_ACL_ACTION_TAG).text = action
        etree.SubElement(ace, migration.XML_ACL_ALLOW_TAG).text = allow
        etree.SubElement(ace, migration.XML_ACL_ROLE_TAG).text = role


def series_exists(series_id, server, auth):
    """ Returns whether or not a series exist in the given server """
    try:
        get_series(series_id, server, auth)
        return True
    except HTTPError as herr:
        if herr.response.status_code == 404:
            return False
        # Otherwise, raise a exception
        raise

def create_file_flag(path):
    """
    Creates a file flag at the given path, creating the necessary directories
    """
    try:
        with open(path, 'w'):
            pass
    except IOError as ioe:
        if ioe.errno == errno.ENOENT:
            # Handle the case when the series is empty
            # If so, the directory was not yet created
            # Any other errors from this point should be raised
            os.makedirs(os.path.dirname(path), config.dir_mode)
            with open(path, 'w'):
                pass
        else:
            raise

def create_zip(zip_name, mp_dir, dst_dir=None):
    """
    Create ZIP file for the mediapackage stored in the directory 'mp_dir'
    Return the path to the created file
    """

    if dst_dir is None:
        dst_dir = mp_dir

    if os.path.splitext(zip_name)[1] != '.zip':
        zip_name = zip_name + '.zip'

    zip_file = os.path.join(dst_dir, zip_name)
    cwd = os.getcwd()
    try:
        os.chdir(dst_dir)
        LOGGER.debug("Creating ZIP file '%s'", zip_file)
        subprocess.check_call(['zip', '-0ru', '--quiet', zip_name] + os.listdir('.'))
        LOGGER.debug("ZIP file created: '%s'", zip_file)
    finally:
        os.chdir(cwd)

    return zip_file


def migrate_mediapackage(mp_xml, root_dir, exporter):
    """
    Migrate a single mediapackage into the provided root directory
    """

    # Get mediapackage ID
    mp_id = mp_xml.get(migration.XML_MP_ID_ATTR)

    LOGGER.debug("Attempting to migrate mediapackage '%s'", mp_id)

    mp_dir = os.path.join(root_dir, mp_id)
    try:
        os.makedirs(mp_dir, config.dir_mode)
    except OSError as ose:
        if ose.errno != errno.EEXIST:
            # Ignore the exception raised when the directory already exists
            raise

    # Calculate file flags for ingested and failed mediapackages
    ingested_file = os.path.join(mp_dir, config.ingested_filename)
    failed_file = os.path.join(mp_dir, config.failed_filename)

    # Make sure this MP was not already ingested
    if os.path.isfile(ingested_file):
        raise migration.IngestedException(
            "Mediapackage '{0}' was already marked as ingested".format(mp_id)
        )

    # Make sure this MP did not fail
    if os.path.isfile(failed_file):
        raise migration.AlreadyFailedException(
            "Mediapackage '{0}' was already marked as failed".format(mp_id)
        )

    # Check the mediapackage has not already been exported
    try:
        migration.get_unique_mp(
            mp_id,
            migration.get_url(config.dst_admin, config.ep_dst_archive_list),
            config.dst_auth)

        # The MP is already ingested
        LOGGER.warn(
            "Mediapackage '%s' is not marked as ingested, "
            "but is already archived in the destination system", mp_id)

        # Mark this MP as ingested
        create_file_flag(ingested_file)

        return

    except migration.NotFoundException:
        # This is expected
        pass

    try:
        exporter.export(
            mp_id,
            filter_by_flavor=config.filter_flavors,
            filter_tags=config.remove_tags
        )

        # Copy files
        for rel_dst, src in exporter.paths.iteritems():
            dst = os.path.join(mp_dir, rel_dst)
            if os.path.exists(dst):
                if filecmp.cmp(src, dst):
                    LOGGER.debug("Path '%s' was copied in an earlier run of the script", dst)
                    continue
            else:
                try:
                    os.makedirs(os.path.dirname(dst), config.dir_mode)
                except OSError as err:
                    # Swallow the error if the directory already exists.
                    # Raise in any other case
                    if err.errno != errno.EEXIST:
                        raise

            # Copy the file
            shutil.copyfile(src, dst)

        # Serialize the manifest
        with open(os.path.join(mp_dir, config.manifest_filename), "w") as manifest_file:
            etree.ElementTree(exporter.mediapackage).write(
                manifest_file, encoding="utf-8", xml_declaration=True, pretty_print=True)

        # Zip the mediapackage
        zip_file = create_zip(mp_id, mp_dir)

        # Copy the zip file in the inbox
        if os.path.isdir(config.inbox):
            shutil.copy(zip_file, config.inbox)
        else:
            raise OSError(errno.ENOENT, "The destination inbox does not exist", config.inbox)

        LOGGER.info("Mediapackage successfully ingested: '%s'", mp_id)

        # Mark this MP as ingested
        create_file_flag(ingested_file)

    except Exception:
        # Mark the mediapackage as failed
        create_file_flag(failed_file)
        raise
    finally:
        # Delete ingested files, if so configured
        if config.delete_ingested:
            for root, dirs, files in os.walk(mp_dir, topdown=False):
                for name in files:
                    full_path = os.path.join(root, name)
                    for check_file in [ingested_file, failed_file]:
                        try:
                            if os.path.samefile(full_path, check_file):
                                continue
                        except OSError as ose:
                            # The file may not exist, so the error can be safely ignored
                            if ose.errno != errno.ENOENT:
                                # Otherwise, raise the exception
                                raise
                    os.remove(full_path)
                for name in dirs:
                    os.rmdir(os.path.join(root, name))



def migrate_mediapackages(series_id, dst_dir, exporter, iterate=False):
    """
    Migrate mediapackages from a series according to the given parameters.
    Returns true if all the episodes in the series are ingested, false otherwise
    """
    LOGGER.debug("Attempting to migrate series %s", series_id)

    series_dir = os.path.join(dst_dir, series_id)

    # Check whether this series was already migrated
    ingested_file = os.path.join(series_dir, config.ingested_filename)
    failed_file = os.path.join(series_dir, config.failed_filename)
    if os.path.isfile(ingested_file):
        raise migration.IngestedException(
            "Series {} is already marked as ingested".format(series_id))
    if os.path.isfile(failed_file):
        raise migration.AlreadyFailedException(
            "Series {} is already marked as failed".format(series_id))

    mp_list = exporter.get_mediapackages_from_series(series_id)
    mp_processed = 0
    failed = False
    while mp_list:
        # Iterate through the results
        for mp_xml in mp_list:
            try:
                migrate_mediapackage(mp_xml, series_dir, exporter)
                if not iterate:
                    return
            except migration.IngestedException as ing_exc:
                # Log as debug and keep going
                # This means only that the MP was already ingested succesfully
                LOGGER.debug(ing_exc)
            except migration.AlreadyFailedException as f_exc:
                # The MP failed and was marked as such
                # The failure was already logged, so we log this as debug
                LOGGER.debug(f_exc)
                failed = True
            except Exception as exc:
                # Log this exception, but keep going.
                LOGGER.error(exc)
                failed = True

        # Update counter
        mp_processed += len(mp_list)
        # Request a new batch of MP
        mp_list = exporter.get_mediapackages_from_series(
            series_id, offset=mp_processed)

    # We reached the end of this series
    # Mark the series as failed or ingested
    if failed:
        LOGGER.info("Marking series %s as FAILED", series_id)
        flag = failed_file
    else:
        LOGGER.info("Marking series %s as INGESTED", series_id)
        flag = ingested_file

    create_file_flag(flag)


def edit_series(series):
    """
    Receive an XML representation of a series and add default metadata values
    """
    series_xml = etree.fromstring(series)

    for key, value in config.series_extra_metadata.iteritems():
        element_xml = etree.SubElement(series_xml, key)
        element_xml.text = value

    return etree.tostring(series_xml, encoding='utf-8', xml_declaration=True, pretty_print=True)


def migrate_single_series(series_id, dst_dir, exporter, iterate=True):
    """ Migrate all the elements in the given series """

    # Check if series exists in the system we migrate from
    if not series_exists(series_id, config.src_admin, config.src_auth):
        raise migration.NotFoundException(
            "Series {} does not exist in the source system".format(series_id))

    # Check if series exists in the system to migrate...
    if not series_exists(series_id, config.dst_admin, config.dst_auth):
        LOGGER.info("Series '%s' does not exist in the destination system. Creating it...",
                    series_id)
        try:
            # Get series in the source system
            src_series = edit_series(get_series(series_id, config.src_admin, config.src_auth))

            # Get series ACL in the source system
            src_series_acl = edit_acl(
                get_series_acl(
                    series_id,
                    config.src_admin,
                    config.src_auth),
                config.acl_default_roles,
                config.acl_transform_roles
            )
            # Create series in the destination system
            create_series(
                src_series,
                src_series_acl,
                config.dst_admin,
                config.dst_auth
            )
        except HTTPError as herr:
            if herr.response.status_code == 404:
                raise migration.NotFoundException(
                    "The series '{0}' does not exist in the source system at {1}".format(
                        series_id, config.src_admin))
            else:
                raise

    migrate_mediapackages(series_id, dst_dir, exporter, iterate)


def migrate_multiple_series(series_file, dst_dir, exporter, iterate=True):
    """ Migrate all series listed in the provided file  """

    with open(series_file, 'r+') as series_file:
        for series_id in series_file.readlines():
            # Remove whitespace
            series_id = series_id.strip().split()[0]
            if series_id.startswith('#'):
                # Comments are ignored
                continue
            try:
                migrate_single_series(series_id, dst_dir, exporter, iterate)
                if not iterate:
                    break
            except migration.IngestedException as exc:
                # This is perfectly fine. We just ignore already ingested series and keep going
                LOGGER.debug("Already ingested: %s", exc)
            except migration.AlreadyFailedException as exc:
                # This is perfectly fine. We just ignore series already marked as failed
                # and keep going
                LOGGER.debug("Already marked as failed: %s", exc)
            except migration.NotFoundException as exc:
                LOGGER.error("Not found: %s", exc)


def __migrate_series(series_param, dst_dir, iterate=True):
    """ Process a request from the command line """

    # TODO configure number of services
    archive_export = migration.ArchiveServiceExport(
        config.src_admin,
        config.src_user, config.src_pass,
        config.archive_dir,
        legacy=True
    )
    publish_export = migration.PublishServiceExport(
        config.src_engage,
        config.src_user, config.src_pass,
        config.search_dirs,
        config.archive_dir
    )
    exporter = migration.Export(archive_export, publish_export)

    if series_param.startswith('@'):
        migrate_multiple_series(series_param.lstrip('@'), dst_dir, exporter, iterate)
    else:
        migrate_single_series(series_param, dst_dir, exporter, iterate)


def __parse_args():

    # Argument parser
    arg_parser = argparse.ArgumentParser(description="Ingest all the mediapackages in a series")

    arg_parser.add_argument(
        'series_param',
        metavar='series',
        help='The identifier of the series to be migrated. If starting with a "@", '
        'it will be interpreted as a filename of which multiple series identifier will be read, '
        'one per line.'
    )
    arg_parser.add_argument(
        'dst_dir',
        help='The path where the files will be exported.'
        'It will be created if it does not exist.'
    )
    arg_parser.add_argument(
        '-i', '--do_not_iterate',
        action="store_false",
        dest='iterate',
        help='Process one mediapackage at a time, then return. '
        'This is useful to run this script using a cronjob, as a method to throttle the ingestions.'
    )
    return arg_parser.parse_args()



if __name__ == '__main__':
    pid = str(os.getpid())
    old_pid = None
    pidfile = None
    try:
        pidfile = open(config.pidfilename, 'r+')
        old_pid = pidfile.readline()
        os.kill(int(old_pid), 0)
        LOGGER.error("An instance of this script is already running as process %s. Aborting...", old_pid)
        sys.exit(1)
    except ValueError as err:
        LOGGER.warn("Found pidfile with invalid pid %s: %s. Going forward...", old_pid, err)
        pidfile.seek(0)
        pidfile.truncate()
        pidfile.write(pid)
    except (IOError, OSError) as err:
        if err.errno in [errno.ESRCH, errno.ENOENT]:
            if err.errno == errno.ESRCH:
                LOGGER.warn("Found pidfile with pid %s but the script is not running. Going forward...", old_pid)
                pidfile.seek(0)
                pidfile.truncate()
            elif err.errno == errno.ENOENT:
                LOGGER.debug("Creating pidfile with pid %s", pid)
                pidfile = open(config.pidfilename, 'w')
            pidfile.write(pid)
        else:
            raise
    finally:
        if pidfile is not None:
            pidfile.close()

    try:
        __migrate_series(**vars(__parse_args()))
        sys.exit(0)
    except migration.MigrationException as exc:
        LOGGER.error("(%s) %s", type(exc).__name__, exc)
        sys.exit(1)
    finally:
        try:
            os.remove(config.pidfilename)
        except OSError:
            pass
