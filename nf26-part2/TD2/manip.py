"""
Module d'import
"""

import csv
import os

def getdata(directory):
    dial = csv.excel
    dial.delimiter = ","
    for fname in os.listdir(directory):
        with open(directory + os.sep + fname) as f:
            for row in csv.DictReader(f, dialect=dial):
                yield dict(row)

def head(directory):
    for l in getdata(directory):
        head = l
        break
    return head