#!/bin/python
"""
Shows a graphical menu where users can select and ingest the series they wish to migrate
"""
import argparse
import errno
import getpass
import io
import sys
import Tkinter as tk

import requests
from requests.auth import HTTPDigestAuth

class OpencastDigestAuth(HTTPDigestAuth):
    """ Implement a digest authentication including the headers required by Opencast """

    def __call__(self, r):
        # Call the parent method
        r = super(OpencastDigestAuth, self).__call__(r)

        # Add Opencast required headers
        r.headers['X-Requested-Auth'] = 'Digest'
        r.headers['X-Opencast-Matterhorn-Authorization'] = 'true'

        return r

# http://tkinter.unpythonic.net/wiki/VerticalScrolledFrame
class VerticalScrolledFrame(tk.Frame):
    """A pure Tkinter scrollable frame that actually works!
    * Use the 'interior' attribute to place widgets inside the scrollable frame
    * Construct and pack/place/grid normally
    * This frame only allows vertical scrolling

    """
    def __init__(self, parent, *args, **kw):
        tk.Frame.__init__(self, parent, *args, **kw)

        # create a canvas object and a vertical scrollbar for scrolling it
        vscrollbar = tk.Scrollbar(self, orient=tk.VERTICAL)
        vscrollbar.pack(fill=tk.Y, side=tk.RIGHT, expand=tk.TRUE)
        canvas = tk.Canvas(self, bd=0, highlightthickness=0,
                           yscrollcommand=vscrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=tk.TRUE)
        vscrollbar.config(command=canvas.yview)

        # reset the view
        canvas.xview_moveto(0)
        canvas.yview_moveto(0)

        # create a frame inside the canvas which will be scrolled with it
        self.interior = interior = tk.Frame(canvas)
        interior_id = canvas.create_window(0, 0, window=interior,
                                           anchor=tk.NW)

        # track changes to the canvas and frame width and sync them,
        # also updating the scrollbar
        def _configure_interior(event):
            # update the scrollbars to match the size of the inner frame
            size = (interior.winfo_reqwidth(), interior.winfo_reqheight())
            canvas.config(scrollregion="0 0 %s %s" % size)
            if interior.winfo_reqwidth() != canvas.winfo_width():
                # update the canvas's width to fit the inner frame
                canvas.config(width=interior.winfo_reqwidth())
        interior.bind('<Configure>', _configure_interior)

        def _configure_canvas(event):
            if interior.winfo_reqwidth() != canvas.winfo_width():
                # update the inner frame's width to fill the canvas
                canvas.itemconfigure(interior_id, width=canvas.winfo_width())
        canvas.bind('<Configure>', _configure_canvas)


# Default series endpoint
SERIES_ENDPOINT = "/series/series.json"

# Default namespace for the results
DC_NS = 'http://purl.org/dc/terms/'

# Query parameter specifying the number of results per page
QUERY_COUNT = 'count'

# Query parameter specifying the page to return
QUERY_PAGE = 'startPage'

# Key in the results, indicating the total number of series available
KEY_TOTAL = 'totalCount'

# Series results per request
PAGE_SIZE = 100

def write_selected_series_to_file(selected_series, titles, output_file):
    """ Write the list of selected series to the corresponding file """

    series_file = None
    with io.open(output_file, 'w+', encoding='utf8') as series_file:
        for key, value in selected_series.iteritems():
            if value.get():
                series_file.write(u"{0} : {1}\n".format(
                    key, titles[key]['title']))

    print "{0} Created!".format(output_file)

def draw_ui(series_dict, output_file, provided_series=None):
    """ Create UI to select Series for ingest """

    selected_series = dict()

    root = tk.Tk()
    root.wm_title("Select Series to Migrate")
    left_frame = VerticalScrolledFrame(root)
    left_frame.pack(fill=tk.BOTH, expand=tk.YES, side=tk.LEFT)

    right_frame = tk.Frame()
    right_frame.pack(fill=tk.BOTH, expand=tk.YES, side=tk.RIGHT)
    button = tk.Button(right_frame,
                       text="Create Selected Series File",
                       command=lambda:
                       write_selected_series_to_file(selected_series, series_dict, output_file))
    button.grid(sticky=tk.N, row=0)

    button = tk.Button(right_frame, text="Select all", command=lambda: _mark_all(True))
    button.grid(sticky=tk.N, row=1)

    button = tk.Button(right_frame, text="Unselect all", command=lambda: _mark_all(False))
    button.grid(sticky=tk.N, row=2)

    button = tk.Button(right_frame, text="Quit!", fg='red', command=root.quit)
    button.grid(sticky=tk.N, row=3)

    label_value = tk.StringVar()
    label = tk.Label(right_frame, textvariable=label_value, fg="green")
    label.grid(sticky=tk.N, row=4)

    def _mark_all(boolean):
        for value in selected_series.values():
            value.set(boolean)
        _update_count()

    def _update_count():
        label_value.set('{0} selected'.format(
            len([x for x in selected_series if selected_series[x].get()])))

    for key, value in series_dict.iteritems():
        selected_series[key] = tk.BooleanVar()
        if provided_series:
            selected_series[key].set(key in provided_series)
        check_button = tk.Checkbutton(left_frame.interior,
                                      text=value['title'],
                                      variable=selected_series[key],
                                      command=_update_count)
        check_button.grid(sticky=tk.NW)

    _update_count()

    root.mainloop()

def main(args):
    """ Main function """

    # Check whether the output file exist
    provided_series = []
    try:
        with io.open(args.output_file, 'r+', encoding='utf8', errors='replace') as series_file:
            provided_series = [x.strip().split()[0] for x in series_file]
    except IOError as ioe:
        if ioe.errno == errno.ENOENT:
            # This is fine, the file may not exist
            pass
        elif ioe.errno == errno.EACCES:
            # This is fine, we may not have permission to read
            print "Warning. Cannot read the output file: {}".format(args.output_file)
        else:
            if ioe.errno == errno.EISDIR:
                print "Error. The provided output file is a directory: {}".format(args.output_file)
            else:
                raise

            return ioe.errno


    # Set digest login parameters
    if not args.digest_user:
        setattr(args, "digest_user", raw_input("Enter the digest authentication user: "))
    if not args.digest_pass:
        setattr(args, "digest_pass", getpass.getpass("Enter the digest authentication password: "))

    # Digest login
    auth = OpencastDigestAuth(args.digest_user, args.digest_pass)

    series_dict = {}

    # Prepare series request
    series_url = args.server_url + SERIES_ENDPOINT
    # "count" is 1 at first because we will do a first request to get the number of series
    query_params = {QUERY_COUNT: 1,
                    QUERY_PAGE: 0}

    # Get series count
    series_result = requests.get(series_url, auth=auth, params=query_params)
    series_result.raise_for_status()

    total_series = int(series_result.json()[KEY_TOTAL])
    print "{0} series found".format(total_series)

    query_params[QUERY_COUNT] = PAGE_SIZE
    while len(series_dict) < total_series:
        series_result = requests.get(series_url, auth=auth, params=query_params)
        series_result.raise_for_status()
        series_result = series_result.json()

        if series_result['catalogs']:
            for series in series_result['catalogs']:
                series_id = series[DC_NS]['identifier'][0]['value']
                # Save metadata in a dictionary
                series_dict[series_id] = {}
                for key, value in series[DC_NS].iteritems():
                    try:
                        series_dict[series_id][key] = value[0]['value']
                    except (KeyError, IndexError):
                        # That's fine, we ignore this key
                        pass

        query_params[QUERY_PAGE] += 1

    # Draw the UI
    draw_ui(series_dict, args.output_file, provided_series)


if __name__ == '__main__':

    #draw_ui(None, None)
    #sys.exit()

    # Argument parser
    parser = argparse.ArgumentParser(description="Create a list of series to migrate")

    parser.add_argument(
        'server_url',
        help='The URL of the server running the series service'
    )
    parser.add_argument(
        'output_file',
        help='The file to write the output to.'
        'If the file exists, the list displayed will match the contents of the file'
    )
    parser.add_argument(
        '-i', '--ignore',
        action='store_true',
        help='If an output file is give, ignore its contents'
        ' (i.e. do not select any series by default)'
    )
    parser.add_argument(
        '-u', '--digest-user',
        help='User to authenticate with the Opencast endpoint in the server'
    )
    parser.add_argument(
        '-p', '--digest-pass',
        help='Password to authenticate with the Opencast endpoint in the server'
    )

    sys.exit(main(parser.parse_args()))
