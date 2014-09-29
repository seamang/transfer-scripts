transfer-scripts
================

This is a temporary collection of scripts for a one-off file transfer; most unlikely to be of any general use.

The data is not public, so encrypted:

cat partial_20140929.csv | openssl enc -aes-256-cbc -e > partial_20140929.csv.enc

openssl aes-256-cbc -d -in filename.enc -out filename
