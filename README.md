transfer-scripts
================

This is a temporary collection of scripts for a one-off file transfer; most unlikely to be of any general use.

The data is not public, so the input files in filelists and output files in metadata are encrypted:

Encrypting:
cat partial_20140929.csv | openssl enc -aes-256-cbc -e > partial_20140929.csv.enc

Decrypting:
openssl aes-256-cbc -d -in partial_20140929.csv.enc -out partial_20140929.csv

Usage
-----
Install requirements :
$ pip install -r requirements.txt

Run the script :
$ ./processLists.py -u username -p password -i input -o output
