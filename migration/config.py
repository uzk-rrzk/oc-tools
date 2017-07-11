#! /bin/python

from utils import OpencastDigestAuth

#########
# Paths #
#########

# Directory where the mediapackage will be temporary copied
# It should have enough free space to hold them
search_copy_dir = "/mnt/opencast3/migration/published_mediapackages"
archive_copy_dir = "/mnt/opencast3/migration/archived_mediapackages"

# Path to the inbox that will trigger the publication of the files published in the source system
search_inbox = "/mnt/opencast3/storage/migrate-publish-inbox/"

# Path to the inbox that will trigger the archival of the files archived in the source system
archive_inbox = "/mnt/opencast3/storage/migrate-archive-inbox/"

# Whether or not to keep the ingested files after ingestion
delete_ingested = False

# Name of the manifest file
manifest_filename = "manifest.xml"

# File used as flag to mark whether a certain MP or series was ingested
ingested_filename = ".ingested"

# File used as flag to mark whether a certain MP failed to ingest or a series have ingest failures
failed_filename = ".failed"

# The mode applied to the created directories
dir_mode = 0o755

# Extension for the SMIL files. Should include the initial '.'
smil_extension = ".smil"

#######################
# URLs to the servers #
#######################

# Source system --system to be migrated
src_admin =
src_engage =

# Destination system  --system to migrate to
dst_admin =
dst_engage =


###############
# Credentials #
###############

# Source system
src_user =
src_pass =

# Destination system
dst_user =
dst_pass =


###################
# Series handling #
###################

# Default roles that all series should have, if not defined in the source system
# This must be a dictionary of the form:
#     - Keys are strings representing the default roles.
#     - Values are in turn again dictionaries, where:
#         - Keys are strings representing actions ("read", "write", etc).
#               * Please note that Opencast handles the actions "read", "write" and "analyze"
#                 only, but if the "analyze" action is found, its value is ignored and a
#                 warn/debug message is logged. Whether or not other values will be ignored or
#                 cause an error is undefined.
#         - Values are either the string 'true' or the string 'false', indicating whether or not
#           the role is allowed to perform the action.
# Please note that the script makes no attempt to convert the provided values to strings. Using
# any other data types in the 'default_roles' dictionary will likely rise a TypeError
# It is highly recommended to use unicode strings, specially if the roles are to have characters
# outside of the usual English alphablet, underscore, etc.
# Should you want to keep the order in which the roles are defined here, you should use an
# OrderedDict class, instead of a normal dictionary
# You may set this parameter to None to deactivate it
acl_default_roles = {
    u"ROLE_ADMIN": {
        u"read": u"true",
        u"write": u"true"
    },
    u"ROLE_ANONYMOUS": {
        u"read": u"true"
    }
}

# Function to transform the roles read in the source ACL to those to be exported to the
# destination ACL
# You may set this parameter to None to deactivate it
def acl_transform_roles(role, actions):
    import re

    if not re.match('^(ROLE|[0-9]+)_', role):
        # Add the LDAP prefix
        return 'LDAP_' + role, actions

    return role, actions

# Dublincore namespace
dc_namesp = "http://purl.org/dc/terms/" 

# Series extra parameters
series_extra_metadata = {
    "{{{0}}}license".format(dc_namesp): "ALLRIGHTS"
}

##################################################
# Locations where the files will be searched for #
##################################################

# The root directory where the mediapackages archived in the source system are stored
archive_dir = "/mnt/opencast/storage/archive/mh_default_org"

# The directories where the source system's distribution services (download, streaming, etc.)
# store their files
search_dirs = [ "/mnt/opencast/storage/downloads", "/mnt/opencast/storage/streaming" ]

#################
# XML arguments #
#################

# Mediapackage namespace
mp_namesp = "http://mediapackage.opencastproject.org"

# Flavors that should be not ingested
filter_flavors = [ 'security/xacml+series', 'security/xacml+episode' ]

# XML tags that should not be ingested
filter_tags = ['{{{0}}}publication'.format(mp_namesp)]

# XML attribute representing the flavor in the MP elements
mp_flavor_attr = 'type'

# XML attribute representing the ID in the MP elements
mp_elem_id_attr = 'id'

# Mediapackage XML tag (including Namespace)
mp_xml_tag = '{{{0}}}mediapackage'.format(mp_namesp)

# URL tag used in mediapackage XML representations (including Namespace)
url_xml_tag = '{{{0}}}url'.format(mp_namesp)

# XML tag for the 'series' XML element in mediapackage representations (including Namespace)
series_xml_tag = '{{{0}}}series'.format(mp_namesp)

# XML tag for the 'tags' XML element in mediapackage XML representations (with Namespace)
tags_xml_tag = '{{{0}}}tags'.format(mp_namesp)

# XML tag for the 'tag' XML element in mediapackage XML representations (with Namespace)
tag_xml_tag = '{{{0}}}tag'.format(mp_namesp)

# XML tag for the 'media' XML element in the mediapackage XML representations (with Namespace)
media_xml_tag = '{{{0}}}media'.format(mp_namesp)

# XML tag for the 'mimetype' XML element in mediapackage XML representations (with Namespace)
mimetype_xml_tag = '{{{0}}}mimetype'.format(mp_namesp)

# XML tag for the 'track' XML element in mediapackage XML representations (with Namespace)
track_xml_tag = '{{{0}}}track'.format(mp_namesp)

# XML tag for the 'publication' element in mediapackage XML representations (with Namespace)
publication_xml_tag = '{{{0}}}publication'.format(mp_namesp)

# XML attribute containing the video URL in smil files
smil_src_attr = 'src'

# Attributes to be removed in elements created from SMIL files
smil_filter_attributes = ['transport']

# Suffix of all element tags representing video qualities
tag_quality_suffix = "-quality"

# Function to tell whether or not a certain tag represents a video quality
def is_quality_tag(tag):
    return tag.endswith(tag_quality_suffix)

# ACL XML namespace
acl_namesp = "http://org.opencastproject.security"

# ACL XML root tag
acl_root = "{{{0}}}acl".format(acl_namesp)

# ACL XML element tag
acl_element = "{{{0}}}ace".format(acl_namesp)

# ACL XML role tag
acl_role = "{{{0}}}role".format(acl_namesp)

# ACL XML action tag
acl_action = "{{{0}}}action".format(acl_namesp)

# ACL XML allow tag
acl_allow = "{{{0}}}allow".format(acl_namesp)

# Function to return total number of elements in a search
def get_total(mp_list_xml):
    return int(mp_list_xml.get('total'))

#############
# Endpoints #
#############

# Endpoint to get a list of the existing published mediapackages
ep_search_list = 'search/episode.xml'
# Endpoint to get a list of the existing archived mediapackages IN THE SOURCE SYSTEM
# This is because the archive endpoints changed between 1.x and 2.x
ep_src_archive_list = 'episode/episode.xml'
# Endpoint to get a list of the existing archived mediapackages IN THE DESTINATION SYSTEM
ep_dst_archive_list = 'archive/episode.xml'
# Endpoint to get a series
ep_series_get = 'series/{sid}.xml'
# Endpoint to get a series' ACL
ep_series_acl = 'series/{sid}/acl.xml'
# Endpoint to create a series
ep_series_post = 'series'

#######################
# Endpoint parameters #
#######################

# Default size of the result pages
page_size = 20

## Query parameters ##
# Mediapackage ID
query_id = 'id'
# Series ID in the search service
query_search_series_id = 'sid'
# Series ID in the archive service
query_archive_series_id = 'series'
# Size of the results queries
query_page_size = 'limit'
# Which page of the results are we requesting
query_page = 'offset'

## Post parameters ##
ep_series_post_series = 'series'
ep_series_post_acl = 'acl'

##################
# Authentication #
##################

# Digest authentication for the source system
src_auth = OpencastDigestAuth(src_user, src_pass)
# Digest authentication for the destination system
dst_auth = OpencastDigestAuth(dst_user, dst_pass)
