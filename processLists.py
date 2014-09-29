#!/usr/bin/python
# Processes a list of arc file names to generate a CSV file of
# metadata for each file
# Assumes the file list groups crawls together in date ascending order

import sys, getopt, re, csv, requests, time, random
from datetime import datetime
from time import strftime
from requests.auth import HTTPBasicAuth

def main(argv):
    uname, pwd, ifname, ofname = getParms()
    # try opening the files    
    try:
        fhi = open(ifname, "rb")
    except IOError:
        print "Error: can\'t find file or read data"
    else:
        try:
            fho = open(ofname, "wb")
            # writer = csv.writer(fho, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            fields = ['identifier','filename','folder','date_created','checksum','series_number','creating_body','crawl_start', 'crawl_end', 'filesize', 'unit']
            writer = csv.DictWriter(fho, delimiter=',', fieldnames=fields)
            writer.writerow(dict((fn,fn) for fn in fields))
        except IOError:
            print "Error: can\'t open output csv file"
        else:
            print "Opened files successfully"
            groupFiles(uname, pwd, fhi, writer)
            fhi.close()
            fho.close()

# Process an input list of Arc filenames, generating one CSV row for
# each filename. Each row contains most of the values that will be needed for the
# metadata (though not the checksum), as well as the allocation of each file to a drive
def groupFiles(uname, pwd, fhi, writer):
    # define constants
    # list of available drives by label
    units = ['246','247','248','249','250','251','252','253','254','255']
    # size of each drive
    unitSize = 1,900,000,000,000 # 1.9 TB (actual is 1,953,378,644,000). Needs python >= 2.5
    general_pattern = re.compile('^(.*)[\-P](\d{6,17})[^\d](\d{1,5})?[^\d].*$')
    tna_extracted_pattern = re.compile('^(.*\-(\d{4}))\-part\-(\d{8}).*$')
    #bl_old_pattern = re.compile('BL-\d{6}(\_\d+)?\-?(\d{8,14})?\-?(\d{5})?\.arc\.gz$')
    #bl_old_pattern = re.compile('BL\-\d{6,8}\.arc\.gz$')
    #bl_new_pattern = re.compile('(BL\-\d{6}(?:\_\d+))\-?(\d{8,14})?\-?(\d{5})?\.arc\.gz$')
    identifierBase = 'file:///T:WORK/RW_32/content/'
    blankRow = {'identifier':'', 'filename':'', 'folder': '', 'date_created':'', 'checksum':'', 'series_number':'', 'creating_body':'IMF', 'crawl_start':'', 'crawl_end':'', 'filesize':'', 'unit':''}
    
    # initialise variables
    # array of files for one folder
    crawl = []
    # folder name
    dir = ''
    # full path to folder
    uriBase = ''
    # date of file
    date = ''
    # count of unparseable filenames
    nonfits = 0
    # total bytes to write to a single drive
    runningTotal = 0
    # current disk drive (drive label is a unit number)
    unit = units.pop(0)

    # Accumulate details for all the files to go in one folder,
    # outputting the details only when the folder is complete.
    for line in fhi:
        # establish the size of the file
        arcSize = getArcSize(uname, pwd, line)
        runningTotal += long(arcSize)
        print runningTotal
        # check whether we neeed to move to a new drive
        if runningTotal >= unitSize:
            unit = units.pop(0)
            runningTotal = arcSize
        # parse the filename
        parts = re.split('/', line)    
        filename = parts[len(parts)-1].rstrip()
        if filename[:14] == 'TNA-EXTRACTED-':
            pattern = tna_extracted_pattern
        else:
            pattern = general_pattern
        parts = re.match(pattern, filename)
        if not parts:  # filename does not match regex - these should be RARE
            # print previous crawl, if any
            printCrawl(writer, crawl, date)
            crawl = []
            dir = filename
            uriBase = identifierBase + dir 
            # make directory of one file, with same name as directory
            row = blankRow.copy()
            row['identifier']= uriBase
            row['filename'] = dir
            row['folder'] = 'folder'
            writer.writerow(row)
            row['identifier'] = uriBase + '/' + filename
            row['filename'] = filename
            row['folder'] = 'file'
            row['checksum'] = '[checksum]'
            row['filesize'] = arcSize
            row['unit'] = unit
            writer.writerow(row)
            nonfits = nonfits + 1 # just diagnostic
            continue
        # otherwise...
        newdir = parts.group(1) # we can parse the filename
        if newdir != dir:
            # print previous crawl, if any
            printCrawl(writer, crawl, date)
            crawl = []
            dir = newdir
            uriBase = identifierBase + dir 
            # create the new folder row
            row = blankRow.copy()
            row['identifier']= uriBase
            row['filename'] = dir
            row['folder'] = 'folder'
            crawlstart = dateConvert(parts.group(2))
            row['crawl_start'] = crawlstart
            crawl.append(row)

        # create the row for the current file
        row = blankRow.copy()
        row['filesize'] = arcSize
        row['identifier'] = uriBase + '/' + filename
        row['filename'] = filename
        row['folder'] = 'file'
        date = dateConvert(parts.group(2))
        row['date_created'] = date
        row['checksum'] = '[checksum]'
        part = parts.group(3)
        if part:
            row['series_number'] = part
        row['crawl_start'] = crawlstart
        row['unit'] = unit
        crawl.append(row)
        # print(row) 

    #need to flush final crawl
    printCrawl(writer, crawl, date)     
    print "nonfits: " + `nonfits`

# find the size of an Arc file by making a HEAD call to the url
# and parsing the result
def getArcSize(uname, pwd, url):
    user_agent = {'User-agent': 'processLists'}
    h = requests.head(url.strip(),headers=user_agent,auth=HTTPBasicAuth(uname, pwd))
    print h.status_code
    if h.status_code == 302:
        print h.headers['Location']
        h = requests.head(h.headers['Location'],headers=user_agent,auth=HTTPBasicAuth(uname, pwd))
    time.sleep(random.randint(1,7))
    if 'content-length' in h.headers:    
        return h.headers['content-length']
    else:
        print h.status_code
        print h.headers
        return 0


# convert multiple date formats to ISO8601
# Probably false assumption made that times are UTC
def dateConvert(date):
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
        

# print all rows within a crawl folder, including the folder itself
def printCrawl(writer, crawl, end_date):
    for row in crawl:
        row['crawl_end'] = end_date
        writer.writerow(row)
        
# get command line parameters        
def getParms():
    ifile = ofile = uname = pwd = ""
    try:
        myopts, args = getopt.getopt(sys.argv[1:],"u:p:i:o:")
    except getopt.GetoptError as e:
        print (str(e))
        usage()
 
    for o, a in myopts:
        if o == '-i':
            ifile = a
        elif o == '-o':
            ofile = a
        elif o == '-u':
            uname = a
        elif o == '-p':
            pwd = a

    if not uname:
        usage()
    if not pwd:
        usage()
    if not ifile:
        usage()
    if not ofile:
        usage()

    return (uname, pwd, ifile, ofile)

def usage():
        print("Usage: %s -u username -p password -i input -o output" % sys.argv[0])
        sys.exit(2)
    

if __name__ == "__main__":
   main(sys.argv[1:])
