import csv
import re
import os

def loadataDirectory(directory):
    dateparser = re.compile(
        r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+):(?P<seconds>\d+\.?\d*)"
    )
    for fname in os.listdir(directory):
        print(fname)
        dial = csv.excel
        dial.delimiter = ","
        with open(directory + os.sep + fname) as f:
            for r in csv.DictReader(f, dialect=dial):
                match_start = dateparser.match(r["starttime"])
                match_stop = dateparser.match(r["stoptime"])
                if not match_start or not match_stop:
                    continue
                start = match_start.groupdict()
                stop = match_stop.groupdict()
                data = {}
                data["tripduration"] = int(r["tripduration"])
                data["starttime"] = (
                    int(start["year"]),
                    int(start["month"]),
                    int(start["day"]),
                    int(start["hour"]),
                    int(start["minute"]),
                    float(start["seconds"]),
                )
                data["stoptime"] = (
                    int(stop["year"]),
                    int(stop["month"]),
                    int(stop["day"]),
                    int(stop["hour"]),
                    int(stop["minute"]),
                    float(stop["seconds"]),
                )
                data["start station name"] = r["start station name"]
                data["start station latitude"] = float(r["start station latitude"])
                data["start station longitude"] = float(r["start station longitude"])
                data["stop station name"] = r["end station name"]
                data["stop station latitude"] = float(r["end station latitude"])
                data["stop station longitude"] = float(r["end station longitude"])
                data["bikeid"] = int(r["bikeid"])
                data["usertype"] = r["usertype"]
                if (isinstance(r["birth year"],  int)):
                    data["birth year"] = int(r["birth year"])
                else: 
                    data["birth year"] = 1980
                data["gender"] = int(r["gender"])
                yield data