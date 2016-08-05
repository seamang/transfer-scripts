#!/usr/bin/python
"""
Add the crawl start date to each folder in the metadata, and move the
actual folder correspondingly
"""

from getopt import getopt, GetoptError
from csv import DictWriter, DictReader
import sys, re, requests, time, random
from datetime import datetime
from time import strftime
from hurry.filesize import size
from os import stat
from shutil import move


IDENTIFIER_BASE = 'file:///T:WORK/RW_32/content/'

def main(argv):
    mountpoint, ofname = getParms()
    if not mountpoint.endswith('/'):
        mountpoint = mountpoint  + '/'
    ifname = mountpoint + 'RW_32/metadata_v7.csv'
    # try opening the files    
    try:
        #scl enable python27 bash
        # to allow multiple openings on one line
        with open(ifname, "rb") as mi, open(ofname, "wb") as mo:
            fields = ['identifier','filename','folder','date_created','checksum', \
                      'series_number','creating_body','crawl_start', 'crawl_end', \
                      'filesize', 'unit']
            reader = DictReader(mi, fieldnames=fields)
            writer = DictWriter(mo, delimiter=',', fieldnames=fields)
            writer.writerow(dict((fn,fn) for fn in fields))
            print "[INFO] Opened files successfully."
            modifyMetadata(mountpoint, reader, writer)
    except IOError as e:
        print "[IOERROR] " + e

def modifyMetadata(mountpoint, reader, writer):

    # default start_date
    start_date = ''
    # skip header row
    next(reader, None)
    for row in reader:
        if row['folder'] == 'folder':
            start_date = row['crawl_start'].split("T")[0]
            old_identifier = row['identifier']
            row['identifier'] = row['identifier'] + '_' + start_date
            row['filename'] = row['filename'] + '_' + start_date
            old_identifier = old_identifier.replace('file:///T:WORK/', mountpoint)
            new_identifier = row['identifier']
            new_identifier = new_identifier.replace('file:///T:WORK/', mountpoint)
            print old_identifier + ' -> ' + new_identifier
            move(old_identifier, new_identifier)
            writer.writerow(row)
        else:
            identifier = row['identifier']
            bits = row['identifier'].split('/')
            bits[len(bits)-2] += '_' + start_date
            row['identifier'] = "/".join(bits)
            writer.writerow(row)
            
def getParms():
    """
    Get command line parameters.
    """
    mountpoint = ofile = ""
    try:
        myopts, args = getopt(sys.argv[1:],"m:o:")
    except GetoptError as e:
        print (str(e))
        usage()
 
    for o, a in myopts:
        if o == '-m':
            mountpoint = a
        elif o == '-o':
            ofile = a

    if not (mountpoint and ofile):
        usage()

    return (mountpoint, ofile)

def usage():
    print("Usage: %s -m mountpoint -o outputmetadata" % sys.argv[0])
    sys.exit(2)


if __name__ == "__main__":
   main(sys.argv[1:])
