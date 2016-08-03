#!/usr/bin/python
"""
Processes a list of arc file names to insert into a CSV file of
metadata for each file, where these files have been accidentally
ommitted from the original metadata.
Assumes the file list groups crawls together in date ascending order,
but the output after this will not be.
"""

from getopt import getopt, GetoptError
from csv import DictWriter, DictReader
import sys, re, requests, time, random
from datetime import datetime
from time import strftime
from requests.auth import HTTPBasicAuth
from hurry.filesize import size
from os import stat
from hashlib import md5


UNIT_SIZE = 1900000000000 # 1.9 TB (actual is 1,953,378,644,000). Needs python >= 2.5
IDENTIFIER_BASE = 'file:///T:WORK/RW_32/content/'

def main(argv):
    uname, pwd, filelist, ifname, ofname = getParms()
    # try opening the files    
    try:
        #scl enable python27 bash
        # to allow multiple openings on one line
        with open(filelist, "rb") as fhl, open(ifname, "rb") as fhi, open(ofname, "wb") as fho:
            # read in the list of filenames to insert
            d = {}
            for line in fhl:
                fname = line.split('/')[-1].rstrip()
                #filename points to folder
                parts = splitFilename(fname)
                crawldir = parts.group(1)
                if crawldir in d:
                    d[crawldir].append(line.rstrip())
                else:
                    d[crawldir] = [line.rstrip()]

            fields = ['identifier','filename','folder','date_created','checksum', \
                      'series_number','creating_body','crawl_start', 'crawl_end', \
                      'filesize', 'unit']
            reader = DictReader(fhi, fieldnames=fields)
            writer = DictWriter(fho, delimiter=',', fieldnames=fields)
            writer.writerow(dict((fn,fn) for fn in fields))
            print "[INFO] Opened files successfully."
            insertFiles(uname, pwd, d, reader, writer)
    except IOError as e:
        print "[IOERROR] " + e

def insertFiles(uname, pwd, files, reader, writer):
    """
    Read in and write out each folder from the input metadata file until
    a folder containing missing files is found; then output the missing files, followed by the
    rest of the folder
    """

    blankRow = {'identifier':'', 'filename':'', 'folder': '', 'date_created':'', \
                'checksum':'', 'series_number':'', 'creating_body':'IMF', \
                'crawl_start':'', 'crawl_end':'', 'filesize':'', 'unit':''}

    # skip header row
    next(reader, None)
    for row in reader:
        writer.writerow(row)
        if row['folder'] == 'folder':
            crawl = row['filename']
            if crawl in files:
                print "found " + crawl
                files_to_output = files[crawl]
                uriBase = IDENTIFIER_BASE + crawl
                for path in files_to_output:
                    row['crawl_start'] = ''
                    row['crawl_end'] = ''
                    filename = path.split('/')[-1]
                    parts = splitFilename(filename)
                    row['filename'] = filename
                    arcSize = getArcSize(path)
                    row['filesize'] = arcSize
                    row['identifier'] = uriBase + '/' + filename
                    row['folder'] = 'file'
                    date = dateConvert(parts.group(2))
                    row['date_created'] = date
                    row['checksum'] = md5sum(path)
                    part = parts.group(3)
                    if part:
                        row['series_number'] = part
                    # unit is already populated
                    writer.writerow(row)

def md5sum(fname):
    hash_md5 = md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def splitFilename(filename):
    if filename[:14] == 'TNA-EXTRACTED-':
        pattern = re.compile('^(.*\-(\d{4}))\-part\-(\d{8}).*$')
    else:
        pattern = re.compile('^(.*)[\-P](\d{6,17})[^\d](\d{1,5})?[^\d].*$')
    parts = re.match(pattern, filename)
    return parts

def getArcSize(filename):
    statinfo = stat(filename)
    return statinfo.st_size

def getArcSizeRemote(uname, pwd, url):
    """
    find the size of an Arc file by making a HEAD call to the url and parsing
    the result.
    """
    user_agent = {'User-agent': 'processLists'}
    h = requests.head(url.strip(),headers=user_agent,auth=HTTPBasicAuth(uname, pwd))
    print "[INFO] HTTP Status : %s" % h.status_code
    if h.status_code == 302:
        print "[INFO] HTTP Location : %s" % h.headers['Location']
        h = requests.head(h.headers['Location'], headers=user_agent, \
                          auth=HTTPBasicAuth(uname, pwd))
    # random sleep between 0.1 and 0.7 second (do not stress the server)
    time.sleep(random.randint(1,7) / 10.0) 
    if 'content-length' in h.headers:
        return h.headers['content-length']
    else:
        print "[ERROR] HTTP Status : %s" % h.status_code
        print "[DEBUG] HTTP Header : %s" % h.headers
        return 0

def dateConvert(date):
    """
    Convert multiple date formats to ISO8601 Probably false assumption made
    that times are UTC.
    """
    # print date
    # just a year
    if len(date) == 4:
        date = date
    elif len(date) == 8:
        date_object = datetime.strptime(date, '%Y%m%d')
        date = strftime('%Y-%m-%d', date_object.timetuple())
    elif len(date) == 12:
        date_object = datetime.strptime(date, '%y%m%d%H%M%S')
        date = strftime('%Y-%m-%dT%H:%M:%SZ', date_object.timetuple())
    elif (len(date) == 14) or (len(date) == 17) :
        date_object = datetime.strptime(date[:14], '%Y%m%d%H%M%S')
        date = strftime('%Y-%m-%dT%H:%M:%SZ', date_object.timetuple())
    else:
        date = '*' + date # should never happen
    return date
        

def getParms():
    """
    Get command line parameters.
    """
    filelist = ifile = ofile = uname = pwd = ""
    try:
        myopts, args = getopt(sys.argv[1:],"u:p:f:i:o:")
    except GetoptError as e:
        print (str(e))
        usage()
 
    for o, a in myopts:
        if o == '-f':
            filelist = a
        if o == '-i':
            ifile = a
        elif o == '-o':
            ofile = a
        elif o == '-u':
            uname = a
        elif o == '-p':
            pwd = a

    if not (uname and pwd and ifile and ofile):
        usage()

    return (uname, pwd, filelist, ifile, ofile)

def usage():
    print("Usage: %s -u username -p password -f filelist -i inputmetadata  -o outputmetadata" % sys.argv[0])
    sys.exit(2)


if __name__ == "__main__":
   main(sys.argv[1:])
