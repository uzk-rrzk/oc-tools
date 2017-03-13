#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function

from lxml import etree
import argparse
import sys
import os

def main(argv=None):

    try:
        document = etree.parse(argv.input_file)
    except Exception as e:
        print("[ERROR] Could not open input file '{0}' for reading: ({1}) {2}".format(argv.input_file, type(e).__name__, e), file=sys.stderr)
        return 1

    odir = argv.output_dir if argv.output_dir else os.path.dirname(argv.input_file)

    if odir:
        try:
            os.makedirs(odir)
        except Exception as e:
            print("[ERROR] Could not create output directory '{0}': ({1}) {2}".format(argv.output_dir, type(e).__name__, e), file=sys.stderr)
            return 2

        os.chdir(odir)

    def_ofname, def_ext = os.path.splitext(os.path.basename(argv.input_file))
    if argv.output_filenames:
        ofname, ext = os.path.splitext(os.path.basename(argv.output_filenames))
    else:
        ofname = def_ofname
        ext = def_ext
              
    ext = ext if ext else def_ext if def_ext else '.xml'

    n = 1
    for result in document.iter('{*}result'):
        with open("{0}_result_{1}{2}".format(ofname, n, ext), 'w') as f:
            etree.ElementTree(result).write(f, pretty_print=True)
        n += 1 



if __name__ == '__main__':

    # Argument parser
    parser = argparse.ArgumentParser(description="Split the XML output of a Matterhorn search and creates separate files for each of the results")
    
    parser.add_argument('input_file', help='An XML file containing the results of a search in the search of episode service in Matterhorn')
    parser.add_argument('-d', '--output_dir', help='The directory where the result files will be copied. Defaults to the location of the input file')
    parser.add_argument('-o', '--output_filenames', 
                        help='The basename of the output files. The suffix ".result-#" will be added to this basename to generate the output filenames. Defaults to the input filename.')

    sys.exit(main(parser.parse_args()))




