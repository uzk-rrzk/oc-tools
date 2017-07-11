#! /bin/python
"""
Utilities to migrate all the episodes in an Opencast/Matterhorn series into another system
"""
from collections import OrderedDict

import argparse
import errno
import logging
import logging.config
import os
import sys

from lxml import etree
import requests
from requests.exceptions import HTTPError

import config
from utils import get_url, IngestedException, NotFoundException, AlreadyFailedException
import migrate_archived
import migrate_published

SERVICES = ('archive', 'publish')
DEFAULT_SERVICE = 'archive'

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
        get_url(server, config.ep_series_post),
        data=post_data,
        auth=auth)
    resp.raise_for_status()
    return resp.content

def get_series(series_id, server, auth):
    """ Reads the series with ID 'series_id' in XML format from the server """
    resp = requests.get(
        get_url(server, config.ep_series_get, sid=series_id),
        auth=auth
    )
    resp.raise_for_status()
    return resp.content


def get_series_acl(series_id, server, auth):
    """ Reads the ACL of the series with ID 'series_id' in XML format """
    resp = requests.get(
        get_url(server, config.ep_series_acl, sid=series_id),
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
    new_acl = etree.Element(config.acl_root, nsmap={None: config.acl_namesp})
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
    for ace in acl_xml.iterfind(config.acl_element):
        role = ace.find(config.acl_role).text
        action = ace.find(config.acl_action).text
        allow = ace.find(config.acl_allow).text

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
        ace = etree.SubElement(acl_xml, config.acl_element)
        etree.SubElement(ace, config.acl_action).text = action
        etree.SubElement(ace, config.acl_allow).text = allow
        etree.SubElement(ace, config.acl_role).text = role


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

def __migrate_mediapackages(series_id, url, auth, migrate, query_series_id, iterate=False):
    """
    Migrate mediapackages from a series according to the given parameters.
    Returns true if all the episodes in the series are ingested, false otherwise
    """

    LOGGER.debug("Start __migrate_mediapackages")

    # Get mediapackages from the episode service
    query = {
        query_series_id: series_id,
        # The size here is 1 only to get the total number of results quickly
        config.query_page_size: 1,
        config.query_page: 0
    }
    resp = requests.get(
        url,
        params=query,
        auth=auth)
    resp.raise_for_status()

    mp_list_xml = etree.fromstring(resp.content)
    total = config.get_total(mp_list_xml)

    # Now get the results
    query[config.query_page_size] = config.page_size

    failed = False
    while query[config.query_page] < total:
        # Get a page of results
        resp = requests.get(
            url,
            params=query,
            auth=auth)
        resp.raise_for_status()

        # Transform the result to XML
        mp_list_xml = etree.fromstring(resp.content)

        # Iterate through the results
        for mp_xml in mp_list_xml.iter(config.mp_xml_tag):
            try:
                migrate(mp_xml)
                if not iterate:
                    return False
            except (IngestedException, AlreadyFailedException):
                # We treat this exception separately because we do not want to log it
                # every time we get it. It is fine for a MP to be already ingested, and any
                # failed ingestion is already logged the first time it happens for a
                # certain mediapackage
                continue
            except Exception as exc:
                # Log this exception, but keep going.
                LOGGER.error(exc)
                failed = True
                continue
            finally:
                # We must increase this count no matter what exceptions we get
                query[config.query_page] += 1

    return not failed


def edit_series(series):
    """
    Receive an XML representation of a series and add default metadata values
    """
    series_xml = etree.fromstring(series)

    for key, value in config.series_extra_metadata.iteritems():
        element_xml = etree.SubElement(series_xml, key)
        element_xml.text = value

    return etree.tostring(series_xml, encoding='utf-8', xml_declaration=True, pretty_print=True)


def migrate_single_series(series_id, service=DEFAULT_SERVICE, iterate=True):
    """ Migrate all the elements in the given series """

    # Check if series exists in the system we migrate from
    if not series_exists(series_id, config.src_admin, config.src_auth):
        raise NotFoundException(
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
                raise NotFoundException(
                    "The series '{0}' does not exist in the source system at {1}".format(
                        series_id, config.src_admin))
            else:
                raise

    if str(service).lower() == 'archive':
        ingested_file = os.path.join(config.archive_copy_dir, series_id, config.ingested_filename)
        migrate_url = get_url(config.src_admin, config.ep_src_archive_list)
        migrate_method = migrate_archived.migrate_archived
        query_key = config.query_archive_series_id
    elif str(service).lower() == 'publish':
        ingested_file = os.path.join(config.search_copy_dir, series_id, config.ingested_filename)
        migrate_url = get_url(config.src_engage, config.ep_search_list)
        migrate_method = migrate_published.migrate_published
        query_key = config.query_search_series_id
    else:
        raise ValueError(
            "Incorrect service parameter: {0}".format(service), file=sys.stderr)

    LOGGER.debug("Attempting to migrate series %s", series_id)

    if os.path.isfile(ingested_file):
        raise IngestedException("Series {} is already marked as ingested".format(series_id))
    elif __migrate_mediapackages(
            series_id,
            migrate_url,
            config.src_auth,
            migrate_method,
            query_key,
            iterate):

        # Mark the series as ingested
        try:
            with open(ingested_file, 'w+'):
                pass
        except IOError as ioe:
            if ioe.errno == errno.ENOENT:
                # Handle the case when the series is empty
                # If so, the directory was not yet created
                # Any other errors from this point should be raised
                os.makedirs(os.path.dirname(ingested_file))
                with open(ingested_file, 'w+'):
                    pass
            else:
                raise


def migrate_multiple_series(series_file, service=DEFAULT_SERVICE, iterate=True):
    """ Migrate all series listed in the provided file  """

    with open(series_file, 'r+') as series_file:
        for series_id in series_file.readlines():
            # Remove whitespace
            series_id = series_id.strip().split()[0]
            if series_id.startswith('#'):
                # Comments are ignored
                continue
            try:
                migrate_single_series(series_id, service, iterate)
                if not iterate:
                    break
            except IngestedException as exc:
                # This is perfectly fine. We just ignore already ingested series and keep going
                LOGGER.debug("Already ingested: %s", exc)
            except NotFoundException as exc:
                LOGGER.error("Not found: %s", exc)


def __migrate_series(series_param, service, iterate=True):
    """ Process a request from the command line """
    if series_param.startswith('@'):
        migrate_multiple_series(series_param.lstrip('@'), service, iterate)
    else:
        migrate_single_series(series_param, service, iterate)


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
        'service',
        nargs='?',
        default=DEFAULT_SERVICE,
        choices=SERVICES,
        help='Specify which service should be populated'
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
    try:
        __migrate_series(**vars(__parse_args()))
        sys.exit(0)
    except Exception as exc:
        LOGGER.error("(%s) %s", type(exc).__name__, exc)
        sys.exit(1)
