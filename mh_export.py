#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function

import sys

try:
    import paramiko
except ImportError:
    print("Required library 'paramiko' not found. Please install it and run this script again",
          file=sys.stderr)
    sys.exit(1)

import pycurl
import urlparse
import urllib
import argparse
import os
import posixpath as urlpath
import re
import getpass
import signal
import errno
import math

from StringIO import StringIO
from lxml import etree

from socket import gaierror


# Location of the config.properties file in the Matterhorn server
DEFAULT_CONF_FILE = "/etc/matterhorn/config.properties"
# Address of the search (get) endpoint
DEFAULT_SEARCH_ENDPOINT='/search/episode.xml'

# Keys in the Matterhorn configuration that indicate the locations where to look for files
LOCATION_KEYS = [ "org.opencastproject.streaming.directory", "org.opencastproject.download.directory" ]

# The mode applied to the created directories
DIRMODE = 0o755

# Namespace to use at the mediapackages
MP_NAMESPACE="http://mediapackage.opencastproject.org"

# Name of the query parameter to specify the series ID
QUERY_PARAM_SERIES_ID = "sid"

# Boolean value to handle keyboard interruptions gracefully
INTERRUPTED = True

# Smils to delete
smils_to_delete = set()
DELETE_SMILS = True

# Handle keyboard interrupts gracefully
def sigint_handler(signal, frame):
    global INTERRUPTED
    if INTERRUPTED:
        interrupted()
    else:
        INTERRUPTED = True
        print('\n\nFinishing the current download before interrupting. Press Crtl-C again to exit immediately.\n')
        sys.stdout.flush()

signal.signal(signal.SIGINT, sigint_handler)

def interrupted():
    print("\nInterrupted by user!\n")
    sys.exit(1)


def curl(server, endpoint, path_params={}, query_params={}, post_params=[], user="", password="", write_to = None, urlencode=True, timeout=None):

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
            if urlencode:
                c.setopt(pycurl.POST, 1)
                c.setopt(pycurl.POSTFIELDS, urllib.urlencode(postfield))
            else:
                c.setopt(pycurl.HTTPPOST, [ (item[0], urllib.quote_plus(item[1])) for item in post_params ])

        c.setopt(pycurl.WRITEFUNCTION, b.write)

        #c.setopt(pycurl.VERBOSE, True)

        c.perform()

        status_code = c.getinfo(pycurl.HTTP_CODE)

        if status_code != 200:
            raise IOError('cURL error in {0}, HTTP status code {1}'.format(urlparse.urlunparse(url), status_code))

        if write_to is None:
            return b.getvalue()

    except Exception as e:
        #print("An exception has occurred: {0}, {1}".format(type(e).__name__, e), file=sys.stderr)
        raise
    finally:
        c.close()
        if write_to is None:
            b.close()


# Get the distribution directories from the remote server's configuration
# If the "extra_dirs" argument is a non-empty sequence, add these dirs to the set
def get_dirs(client, keys, conf_file, extra_dirs):

    dirs = set()
    for key in keys:
        chan = client.get_transport().open_session()
        chan.exec_command("grep '^{0}' '{1}'".format(re.escape(key), conf_file))

        if chan.recv_exit_status() == 0:
            # Found the key. Get the latest occurrence of it
            for line in chan.makefile().readlines():
                pass
            value = re.sub('^\s*{0}\s*='.format(key), "", line).strip()

            if value:
                dirs.add(value)
            else:
                print(u"WARNING: '{0}' property is empty".format(key), file=sys.stderr)
        else:
            # An error has occurred
            raise RuntimeError("ERROR: Fetching the distribution directories returned error code {0}: {1}".format(chan.recv_exit_status(), chan.makefile_stderr().read()))

    if extra_dirs:
        for d in extra_dirs:
            if d:
                dirs.add(d)

    return dirs


# Remove the server "mountpoint" from a track's URL
def get_relative_path(path):

    # If the path contains a "tag" (streaming URL), remove it and process the path accordingly
    prefix, sep, suffix = path.partition(':')
    if sep == ':':
        # There is a "tag" in the URL. Separate the tag and whatever comes before it
        prefix, tag = urlpath.split(prefix)

        # Reconstruct the URL removing the "tag" part
        clean_path = urlpath.join(prefix, suffix)

        # Check if this is a "smil" tag --then we are dealing with the URL to an adaptive streaming catalog
        if tag == "smil":
            clean_path = urlpath.dirname(clean_path)

        ext = urlpath.splitext(clean_path)[1]
        if ext == "":
            clean_path = clean_path + '.' + tag
        elif ext != '.' + tag:
            print(u"WARNING: Found conflicting tag in path '{0}'. Ignoring the tag '{1}'".format(path, tag),
                  file = sys.stderr)
    else:
        clean_path = path

    # Get the path extension
    ext = urlpath.splitext(clean_path)[1]
    if ext == "":
        print(u"WARNING: Found URL without extension: {0}".format(url))

    # Extract the download server "mountpoint"
    # Matterhorn resource URLs in distributed mediapackage take the form:
    #    organization-id/distribution-channel/mediapackage-id/element-id/filename.extension
    # , therefore, anything that is beyond these four levels in the hierarchy
    # is a part of the download server "mountpoint"
    # The exception to this rule are the Wowza SMIL files, which are directly in the root
    url_path = urlpath.dirname(clean_path)
    if ext != ".smil":
        for i in range(3):
            url_path = urlpath.dirname(url_path)

    # Remove the "mountpoint" from the resource's path to get the system path
    return os.path.normpath(urlpath.relpath(clean_path, url_path))


def get_relative_path_from_url(url):

    # Get this track's URL and parse it
    url_parsed = urlparse.urlparse(url)

    return get_relative_path(url_parsed.path)


def download_path(scp, path, download_dir, dirs):

    global INTERRUPTED, smils_to_delete

    if INTERRUPTED:
        interrupted()

    # Check the extension
    ext = urlpath.splitext(path)[1]

    if ext == ".smil":
        # Append the relative path to the local download directory
        local_path = os.path.join(download_dir, path)
        smils_to_delete.add(local_path)
    else:
        # Remove the two latest directory levels (filename and element ID)
        reduced_path = path
        for i in range(2):
            reduced_path = os.path.dirname(reduced_path)

        local_path = os.path.join(download_dir, os.path.relpath(path, reduced_path))

    if os.path.exists(local_path):
        print(u"Skipping the download of already-existing path: {0}".format(local_path))
        return

    try:
        # Attempt to create the local directories
        os.makedirs(os.path.dirname(local_path), DIRMODE)
    except OSError as e:
        if e.errno == errno.EEXIST:
            if os.path.dirname(local_path) != download_dir:
                # The directory already exists. Assume this file have already been downloaded
                print(u"WARN: Tried to create an already-existing directory: '{0}'.\nSkipping download...".format(os.path.dirname(local_path)), file=sys.stderr)
                return
        else:
            # Raise the exception in any other case
            raise

    # Try to find the relative path in one of the directories read in the configuration
    for root in dirs:
        try:
            # Try to fetch the remote the remote file
            scp.get(os.path.join(root, path),
                    local_path,
                    recursive=True)

            # File correctly downloaded
            print("Done!")

            if INTERRUPTED:
                interrupted()

            # If this is a SMIL file, try and download its contents
            if ext is not None and ext == ".smil":
                print(u"This was a SMIL file. We proceed to download the files inside it.")
                with open(local_path, "r+") as f:
                    smil = etree.parse(f)

                for xml_element in smil.iter("video"):
                    download_path(scp, get_relative_path(xml_element.get("src")), download_dir, dirs)

                print(u"Finished downloading media files in the SMIL file '{0}'".format(path))

            # We assume the first correct download is the only one possible, so we break
            break
        except SCPException:
            # No problem. The file may not exist in that directory. Ignore.
            pass
    else:
        print(u"The file '{0}' could not be found in the configured locations".format(path), file=sys.stderr)


def get_unique_path(path):
    """
    If the path already exists, add a suffix. Return the first non-existing path found
    """
    i = 0
    end_path = path

    while os.path.exists(end_path):
        i += 1
        end_path = path + "({0})".format(i)

    return end_path

BYTE_UNITS = " kMGTPEZY"
EXP_MAX = len(BYTE_UNITS)*10

def convert_si(number):

    suffix = ""
    reduced = 0
    index = 0

    if number:
        index = int(math.log(number,2) // 10)
        exp = index * 10
        if index:
            try:
                suffix = BYTE_UNITS[index]
                reduced = number / float(2 ** exp)
            except IndexError:
                suffix = BYTE_UNITS[len(BYTE_UNITS)-1]
                reduced = number / float(2 ** EXP_MAX)
        else:
            suffix = ""
            reduced = number

    if float(reduced).is_integer():
        return "{0} {1}{2}B".format(reduced, suffix, "i" if index else "")
    else:
        return "{0:.2f} {1}{2}B".format(reduced, suffix, "i" if index else "")

def progress(filename, size, sent):
    short=filename
    for i in range(2):
        short = os.path.dirname(short)
    if short != '':
        filename = os.path.relpath(filename, short)

    print(u"\rDownloading file {0} ({1:.0f}% of {2})...".format(filename, 100 if size == 0 else float(sent*100)/size, convert_si(size)), end=" ")

######################################################################################################################################
######################################################################################################################################
########################################################### CODE OF SCPCLIENT ########################################################
######################################################################################################################################
######################################################################################################################################

# scp.py
# Copyright (C) 2008 James Bardin <j.bardin@gmail.com>

"""
Utilities for sending files over ssh using the scp1 protocol.
"""

#__version__ = '0.9.0'

import locale
import os
import re
from socket import timeout as SocketTimeout


# this is quote from the shlex module, added in py3.3
_find_unsafe = re.compile(br'[^\w@%+=:,./~-]').search


def _sh_quote(s):
    """Return a shell-escaped version of the string `s`."""
    if not s:
        return b""
    if _find_unsafe(s) is None:
        return s

    # use single quotes, and put single quotes into double quotes
    # the string $'b is then quoted as '$'"'"'b'
    return b"'" + s.replace(b"'", b"'\"'\"'") + b"'"


# Unicode conversion functions; assume UTF-8

def asbytes(s):
    """Turns unicode into bytes, if needed.

    Assumes UTF-8.
    """
    if isinstance(s, bytes):
        return s
    else:
        return s.encode('utf-8')


def asunicode(s):
    """Turns bytes into unicode, if needed.

    Uses UTF-8.
    """
    if isinstance(s, bytes):
        return s.decode('utf-8', 'replace')
    else:
        return s


# os.path.sep is unicode on Python 3, no matter the platform
bytes_sep = asbytes(os.path.sep)


# Unicode conversion function for Windows
# Used to convert local paths if the local machine is Windows

def asunicode_win(s):
    """Turns bytes into unicode, if needed.
    """
    if isinstance(s, bytes):
        return s.decode(locale.getpreferredencoding())
    else:
        return s


class SCPClient(object):
    """
    An scp1 implementation, compatible with openssh scp.
    Raises SCPException for all transport related errors. Local filesystem
    and OS errors pass through.

    Main public methods are .put and .get
    The get method is controlled by the remote scp instance, and behaves
    accordingly. This means that symlinks are resolved, and the transfer is
    halted after too many levels of symlinks are detected.
    The put method uses os.walk for recursion, and sends files accordingly.
    Since scp doesn't support symlinks, we send file symlinks as the file
    (matching scp behaviour), but we make no attempt at symlinked directories.
    """
    def __init__(self, transport, buff_size=16384, socket_timeout=5.0,
                 progress=None, sanitize=_sh_quote):
        """
        Create an scp1 client.

        @param transport: an existing paramiko L{Transport}
        @type transport: L{Transport}
        @param buff_size: size of the scp send buffer.
        @type buff_size: int
        @param socket_timeout: channel socket timeout in seconds
        @type socket_timeout: float
        @param progress: callback - called with (filename, size, sent) during
            transfers
        @param sanitize: function - called with filename, should return
            safe or escaped string.  Uses _sh_quote by default.
        @type progress: function(string, int, int)
        """
        self.transport = transport
        self.buff_size = buff_size
        self.socket_timeout = socket_timeout
        self.channel = None
        self.preserve_times = False
        self._progress = progress
        self._recv_dir = b''
        self._rename = False
        self._utime = None
        self.sanitize = sanitize
        self._dirtimes = {}

    def put(self, files, remote_path=b'.',
            recursive=False, preserve_times=False):
        """
        Transfer files to remote host.

        @param files: A single path, or a list of paths to be transfered.
            recursive must be True to transfer directories.
        @type files: string OR list of strings
        @param remote_path: path in which to receive the files on the remote
            host. defaults to '.'
        @type remote_path: str
        @param recursive: transfer files and directories recursively
        @type recursive: bool
        @param preserve_times: preserve mtime and atime of transfered files
            and directories.
        @type preserve_times: bool
        """
        self.preserve_times = preserve_times
        self.channel = self.transport.open_session()
        self._pushed = 0
        self.channel.settimeout(self.socket_timeout)
        scp_command = (b'scp -t ', b'scp -r -t ')[recursive]
        self.channel.exec_command(scp_command +
                                  self.sanitize(asbytes(remote_path)))
        self._recv_confirm()

        if not isinstance(files, (list, tuple)):
            files = [files]

        if recursive:
            self._send_recursive(files)
        else:
            self._send_files(files)

        if self.channel:
            self.channel.close()

    def get(self, remote_path, local_path='',
            recursive=False, preserve_times=False):
        """
        Transfer files from remote host to localhost

        @param remote_path: path to retreive from remote host. since this is
            evaluated by scp on the remote host, shell wildcards and
            environment variables may be used.
        @type remote_path: str
        @param local_path: path in which to receive files locally
        @type local_path: str
        @param recursive: transfer files and directories recursively
        @type recursive: bool
        @param preserve_times: preserve mtime and atime of transfered files
            and directories.
        @type preserve_times: bool
        """
        if not isinstance(remote_path, (list, tuple)):
            remote_path = [remote_path]
        remote_path = [self.sanitize(asbytes(r)) for r in remote_path]
        self._recv_dir = local_path or os.getcwd()
        self._rename = (len(remote_path) == 1 and
                        not os.path.isdir(os.path.abspath(local_path)))
        if len(remote_path) > 1:
            if not os.path.exists(self._recv_dir):
                raise SCPException("Local path '%s' does not exist" %
                                   asunicode(self._recv_dir))
            elif not os.path.isdir(self._recv_dir):
                raise SCPException("Local path '%s' is not a directory" %
                                   asunicode(self._recv_dir))
        rcsv = (b'', b' -r')[recursive]
        prsv = (b'', b' -p')[preserve_times]
        self.channel = self.transport.open_session()
        self._pushed = 0
        self.channel.settimeout(self.socket_timeout)
        self.channel.exec_command(b"scp" +
                                  rcsv +
                                  prsv +
                                  b" -f " +
                                  b' '.join(remote_path))
        self._recv_all()

        if self.channel:
            self.channel.close()

    def _read_stats(self, name):
        """return just the file stats needed for scp"""
        if os.name == 'nt':
            name = asunicode(name)
        stats = os.stat(name)
        mode = oct(stats.st_mode)[-4:]
        size = stats.st_size
        atime = int(stats.st_atime)
        mtime = int(stats.st_mtime)
        return (mode, size, mtime, atime)

    def _send_files(self, files):
        for name in files:
            basename = asbytes(os.path.basename(name))
            (mode, size, mtime, atime) = self._read_stats(name)
            if self.preserve_times:
                self._send_time(mtime, atime)
            file_hdl = open(name, 'rb')

            # The protocol can't handle \n in the filename.
            # Quote them as the control sequence \^J for now,
            # which is how openssh handles it.
            self.channel.sendall(("C%s %d " % (mode, size)).encode('ascii') +
                                 basename.replace(b'\n', b'\\^J') + b"\n")
            self._recv_confirm()
            file_pos = 0
            if self._progress:
                if size == 0:
                    # avoid divide-by-zero
                    self._progress(basename, 1, 1)
                else:
                    self._progress(basename, size, 0)
            buff_size = self.buff_size
            chan = self.channel
            while file_pos < size:
                chan.sendall(file_hdl.read(buff_size))
                file_pos = file_hdl.tell()
                if self._progress:
                    self._progress(basename, size, file_pos)
            chan.sendall('\x00')
            file_hdl.close()
            self._recv_confirm()

    def _chdir(self, from_dir, to_dir):
        # Pop until we're one level up from our next push.
        # Push *once* into to_dir.
        # This is dependent on the depth-first traversal from os.walk

        # add path.sep to each when checking the prefix, so we can use
        # path.dirname after
        common = os.path.commonprefix([from_dir + bytes_sep,
                                       to_dir + bytes_sep])
        # now take the dirname, since commonprefix is character based,
        # and we either have a seperator, or a partial name
        common = os.path.dirname(common)
        cur_dir = from_dir.rstrip(bytes_sep)
        while cur_dir != common:
            cur_dir = os.path.split(cur_dir)[0]
            self._send_popd()
        # now we're in our common base directory, so on
        self._send_pushd(to_dir)

    def _send_recursive(self, files):
        for base in files:
            if not os.path.isdir(base):
                # filename mixed into the bunch
                self._send_files([base])
                continue
            last_dir = asbytes(base)
            for root, dirs, fls in os.walk(base):
                self._chdir(last_dir, asbytes(root))
                self._send_files([os.path.join(root, f) for f in fls])
                last_dir = asbytes(root)
            # back out of the directory
            while self._pushed > 0:
                self._send_popd()

    def _send_pushd(self, directory):
        (mode, size, mtime, atime) = self._read_stats(directory)
        basename = asbytes(os.path.basename(directory))
        if self.preserve_times:
            self._send_time(mtime, atime)
        self.channel.sendall(('D%s 0 ' % mode).encode('ascii') +
                             basename.replace(b'\n', b'\\^J') + b'\n')
        self._recv_confirm()
        self._pushed += 1

    def _send_popd(self):
        self.channel.sendall('E\n')
        self._recv_confirm()
        self._pushed -= 1

    def _send_time(self, mtime, atime):
        self.channel.sendall(('T%d 0 %d 0\n' % (mtime, atime)).encode('ascii'))
        self._recv_confirm()

    def _recv_confirm(self):
        # read scp response
        msg = b''
        try:
            msg = self.channel.recv(512)
        except SocketTimeout:
            raise SCPException('Timout waiting for scp response')
        # slice off the first byte, so this compare will work in python2 and python3
        if msg and msg[0:1] == b'\x00':
            return
        elif msg and msg[0:1] == b'\x01':
            raise SCPException(asunicode(msg[1:]))
        elif self.channel.recv_stderr_ready():
            msg = self.channel.recv_stderr(512)
            raise SCPException(asunicode(msg))
        elif not msg:
            raise SCPException('No response from server')
        else:
            raise SCPException('Invalid response from server', msg)

    def _recv_all(self):
        # loop over scp commands, and receive as necessary
        command = {b'C': self._recv_file,
                   b'T': self._set_time,
                   b'D': self._recv_pushd,
                   b'E': self._recv_popd}
        while not self.channel.closed:
            # wait for command as long as we're open
            self.channel.sendall('\x00')
            msg = self.channel.recv(1024)
            if not msg:  # chan closed while recving
                break
            assert msg[-1:] == b'\n'
            msg = msg[:-1]
            code = msg[0:1]
            try:
                command[code](msg[1:])
            except KeyError:
                raise SCPException(asunicode(msg[1:]))
        # directory times can't be set until we're done writing files
        self._set_dirtimes()

    def _set_time(self, cmd):
        try:
            times = cmd.split(b' ')
            mtime = int(times[0])
            atime = int(times[2]) or mtime
        except:
            self.channel.send(b'\x01')
            raise SCPException('Bad time format')
        # save for later
        self._utime = (atime, mtime)

    def _recv_file(self, cmd):
        chan = self.channel
        parts = cmd.strip().split(b' ', 2)

        try:
            mode = int(parts[0], 8)
            size = int(parts[1])
            if self._rename:
                path = self._recv_dir
                self._rename = False
            elif os.name == 'nt':
                path = os.path.join(asunicode_win(self._recv_dir),
                                    parts[2].decode('utf-8'))
            else:
                path = os.path.join(asbytes(self._recv_dir),
                                    parts[2])
        except:
            chan.send('\x01')
            chan.close()
            raise SCPException('Bad file format')

        try:
            file_hdl = open(path, 'wb')
        except IOError as e:
            chan.send(b'\x01' + str(e).encode('utf-8'))
            chan.close()
            raise

        if self._progress:
            if size == 0:
                # avoid divide-by-zero
                self._progress(path, 1, 1)
            else:
                self._progress(path, size, 0)
        buff_size = self.buff_size
        pos = 0
        chan.send(b'\x00')
        try:
            while pos < size:
                # we have to make sure we don't read the final byte
                if size - pos <= buff_size:
                    buff_size = size - pos
                file_hdl.write(chan.recv(buff_size))
                pos = file_hdl.tell()
                if self._progress:
                    self._progress(path, size, pos)

            msg = chan.recv(512)
            if msg and msg[0:1] != b'\x00':
                raise SCPException(asunicode(msg[1:]))
        except SocketTimeout:
            chan.close()
            raise SCPException('Error receiving, socket.timeout')

        file_hdl.truncate()
        try:
            os.utime(path, self._utime)
            self._utime = None
            os.chmod(path, mode)
            # should we notify the other end?
        finally:
            file_hdl.close()
        # '\x00' confirmation sent in _recv_all

    def _recv_pushd(self, cmd):
        parts = cmd.split(b' ', 2)
        try:
            mode = int(parts[0], 8)
            if self._rename:
                path = self._recv_dir
                self._rename = False
            elif os.name == 'nt':
                path = os.path.join(asunicode_win(self._recv_dir),
                                    parts[2].decode('utf-8'))
            else:
                path = os.path.join(asbytes(self._recv_dir),
                                    parts[2])
        except:
            self.channel.send(b'\x01')
            raise SCPException('Bad directory format')
        try:
            if not os.path.exists(path):
                os.mkdir(path, mode)
            elif os.path.isdir(path):
                os.chmod(path, mode)
            else:
                raise SCPException('%s: Not a directory' % path)
            self._dirtimes[path] = (self._utime)
            self._utime = None
            self._recv_dir = path
        except (OSError, SCPException) as e:
            self.channel.send(b'\x01' + asbytes(str(e)))
            raise

    def _recv_popd(self, *cmd):
        self._recv_dir = os.path.split(self._recv_dir)[0]

    def _set_dirtimes(self):
        try:
            for d in self._dirtimes:
                os.utime(d, self._dirtimes[d])
        finally:
            self._dirtimes = {}


class SCPException(Exception):
    """SCP exception class"""
    pass


######################################################################################################################################
######################################################################################################################################
###################################################### END OF THE CODE OF SCPCLIENT ##################################################
######################################################################################################################################
######################################################################################################################################


def main(args):

    global INTERRUPTED, progress

    try:

        # Process server URL
        search_url = urlparse.urlparse(args.server_url, 'http')
        if not search_url.netloc:
            # The URLs without protocol need to be preceded by // for urlparse to interpret them correctly
            search_url = urlparse.urlparse("//" + args.server_url, 'http')

        if args.ssh_url:
            # An specific URL for the SSH connection was provided
            ssh_url = args.ssh_url
        else:
            # Use the server URL for the SSH connections, too
            # Remove the port, if any
            ssh_url = re.sub(":[0-9]+$", "", search_url.netloc)


        # Create a SSH session to the server
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Try passwordless authentication first
            ssh.connect(ssh_url, username=args.ssh_user)
        except paramiko.SSHException:
            # If that fails, request the password to log in
            prompt = "Enter the SSH password for user '{0}' at {1}: ".format(args.ssh_user if args.ssh_user else getpass.getuser(), ssh_url)
            ssh.connect(ssh_url, username=args.ssh_user, password=getpass.getpass(prompt))

        # Create the SCP client to get the files
        scp = SCPClient(ssh.get_transport(), progress=progress)

        # Get the directories where to look for the files to download
        dirs = get_dirs(ssh, LOCATION_KEYS, args.config, args.extra_dirs)

        # Read the elements published in the search index and create an XML document tree out of the response
        if not args.digest_user:
            setattr(args, "digest_user", raw_input("Enter the digest authentication user: "))

        mp_list_str = curl(search_url,
                           args.endpoint,
                           query_params={ QUERY_PARAM_SERIES_ID: args.series_id },
                           user=args.digest_user,
                           password=getpass.getpass("Enter the digest authentication password: "))

        document = etree.fromstring(mp_list_str)

        # Do not interrupt the program immediately after a keyboard interrupt
        INTERRUPTED = False

        print()

        if args.tags is not None:
            args.tags = set(args.tags)

        # For every mediapackage in the results...
        mp = None
        for mp in document.iter('{{{0}}}mediapackage'.format(MP_NAMESPACE)):
            if INTERRUPTED:
                interrupted()
            mp_title = mp.find('{{{0}}}title'.format(MP_NAMESPACE)).text.replace("/", "_")
            mp_dir = get_unique_path(os.path.join(args.download_dir, mp_title))

            matching_tracks = [ track for track in mp.iter('{{{0}}}track'.format(MP_NAMESPACE))
                                if (not args.flavors or track.get("type") in args.flavors) and
                                (not args.tags or args.tags.intersection([ tag.text for tag in track.iterfind('.//{{{0}}}tag'.format(MP_NAMESPACE))]))]

            # Iterate through the tracks in this mediapackage
            if matching_tracks:
                for track in matching_tracks:
                    if INTERRUPTED:
                        interrupted()

                    # Get this track's URL
                    track_url = track.find('{{{0}}}url'.format(MP_NAMESPACE)).text

                    # Get the relative path of the resource in the remote server
                    rel_path = get_relative_path_from_url(track_url)

                    download_path(scp, rel_path, mp_dir, dirs)

                print()
            else:
                print(u"No matching tracks found in mediapackage '{0}': {1}\n".format(mp.get("id"), mp_title), file=sys.stderr)

        if mp is None:
            print(u"The search returned no mediapackages for the series '{0}".format(args.series_id))

        if DELETE_SMILS:
            for smil in smils_to_delete:
                try:
                    os.remove(smil)
                except Exception as e:
                    print(u"Received exception {0} while deleting path '{1}': {2}".format(e.__class__.__name__, smil, e))

    except pycurl.error as err:
        print(u"ERROR: Could not get the list of published mediapackages in the series '{0}': {1}".format(args.series_id, err),
              file=sys.stderr)
        return 1
    except gaierror as err:
        print(u"ERROR: Could not establish an SSH connection with '{0}': {1}".format(ssh_url, err),
              file=sys.stderr)
        return 1
    except Exception as exc:
        print(u"ERROR ({0}): {1}".format(type(exc).__name__, exc), file=sys.stderr)
        return 1

# Custom action to check the directory provided as a parameter
class checkdir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # Try to create the output directory
        try:
            os.makedirs(values, DIRMODE)
        except OSError:
            # If the path exists, and it's a directory, check if it's empty
            if os.path.isdir(values):
                if os.listdir(values):
                    raise argparse.ArgumentError(self, "The directory '{0}' is not empty.".format(values))
            elif os.path.exists(values):
                raise argparse.ArgumentError(self, "'{0}' already exists and is not a directory".format(values))
            else:
                raise argparse.ArgumentError(self, "Unable to create directory '{0}'. Please check whether the path is valid and the script has permission to create it".format(values))

        setattr(namespace, self.dest, values)


if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Download all the published videos in a Matterhorn series")

    parser.add_argument('server_url', help='The URL of the engage server where the videos will be downloaded from')
    parser.add_argument('series_id', help='The ID of the series to which the videos that should be downloaded belong')
    # We are only interested in file names, but this way the parser makes sure those files exist
    parser.add_argument('download_dir', action=checkdir, help='The destination directory name. It must not exist or be empty')
    parser.add_argument('-s', '--ssh_url', help='The SSH-reachable server URL, if the public URL does not allow it')
    parser.add_argument('-u', '--ssh_user', help='The SSH user to connect to the server')
    parser.add_argument('-c', '--config', default=DEFAULT_CONF_FILE, help='Absolute path of the Matterhorn configuration file in the remote server. (Default: ''{0}'')'
                        .format(DEFAULT_CONF_FILE))
    parser.add_argument('-e', '--endpoint', default=DEFAULT_SEARCH_ENDPOINT,
                        help='Endpoint, relative to the server URL, that should return the mediapackages belonging to the provided series. (Default: ''{0}'')'
                        .format(DEFAULT_SEARCH_ENDPOINT))
    parser.add_argument('-U', '--digest_user', help='User to authenticate with the Matterhorn endpoint in the server')
    parser.add_argument('-d', '--directory', action="append", dest="extra_dirs",
                        help='Add an additional directory where the media files will be searched for. Can be specified several times.\n\
                        Please note that the directories \'download.dir\' and \'streaming.dir\' in the Matterhorn server configuration \
                        will always be inspected by default')
    parser.add_argument('-f', '--flavor', action="append", dest="flavors",
                        help='Download only the elements with the indicated flavor. It can be specified several times, in order to download\n\
                        elements with different flavors')
    parser.add_argument('-t', '--tag', action="append", dest="tags",
                        help='Download only the elements with the indicated tag. It can be specified several times, in order to download\n\
                        elements with different tags or restrict the number of elements matched by the \'--flavor\' parameter')

#    print(parser.parse_args())
#    exit(0)

    sys.exit(main(parser.parse_args()))
