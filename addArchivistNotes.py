#!/usr/bin/python
"""
Add archivist_note and date columns to metadata, and merge with csv list
of bad arcs 
"""

from getopt import getopt, GetoptError
from csv import DictWriter, DictReader, reader
import sys, re, requests, time, random
from datetime import datetime
from time import strftime
from hurry.filesize import size
from os import stat
from shutil import move


IDENTIFIER_BASE = 'file:///T:WORK/RW_32/content/'

def main(argv):
    mountpoint, ifname, ofname = getParms()
    if not mountpoint.endswith('/'):
        mountpoint = mountpoint  + '/'
    metaname = mountpoint + 'RW_32/metadata_v7.csv'
    # try opening the files    
    try:
        #scl enable python27 bash
        # to allow multiple openings on one line
        with open(metaname, "rb") as mi, open(ifname, "rb") as cl, open(ofname, "wb") as mo:
            fields = ['identifier','filename','folder','date_created','checksum', \
                      'series_number','creating_body','crawl_start', 'crawl_end', \
                      'filesize', 'unit']
            all_fields = fields + ['date_archivist_note', 'archivist_note']
            metareader = DictReader(mi, fieldnames=fields)
            creader = reader(cl)
            # will always be tiny wrt metadata so slurp
            corrupt = {}
            for row in creader:
                corrupt[row[0]] = [row[1], row[2]]
            writer = DictWriter(mo, delimiter=',', fieldnames=all_fields)
            writer.writerow(dict((fn,fn) for fn in all_fields))
            print "[INFO] Opened files successfully."
            modifyMetadata(metareader, corrupt, writer)
    except IOError as e:
        print "[IOERROR] " + e

def modifyMetadata(metareader, corrupt, writer):

    # default start_date
    start_date = ''
    # skip header row
    next(metareader, None)
    for row in metareader:
        if row['folder'] == 'folder':
            row['date_archivist_note'] = ''
            row['archivist_note'] = ''
        else:
            filename = row['filename']
            if filename in corrupt:
                row['date_archivist_note'] = corrupt[filename][0]
                row['archivist_note'] = corrupt[filename][1]
            else:
                row['date_archivist_note'] = ''
                row['archivist_note'] = ''
        writer.writerow(row)
            
def getParms():
    """
    Get command line parameters.
    """
    mountpoint = ifile = ofile = ""
    try:
        myopts, args = getopt(sys.argv[1:],"m:i:o:")
    except GetoptError as e:
        print (str(e))
        usage()
 
    for o, a in myopts:
        if o == '-m':
            mountpoint = a
        elif o == '-i':
            ifile = a
        elif o == '-o':
            ofile = a

    if not (mountpoint and ifile and ofile):
        usage()

    return (mountpoint, ifile, ofile)

def usage():
    print("Usage: %s -m mountpoint -i corrupt_list -o outputmetadata" % sys.argv[0])
    sys.exit(2)


if __name__ == "__main__":
   main(sys.argv[1:])
