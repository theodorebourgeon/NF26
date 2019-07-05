import csv
import re


def loadata(filename):
    dateparser = re.compile(
        r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+):(?P<seconds>\d+\.?\d*)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
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
            data["birth year"] = int(r["birth year"])
            data["gender"] = int(r["gender"])
            yield data


import json
import lmdb


class TimeKey:
    def __init__(self, **kwargs):
        if "binrepr" in kwargs:
            b = kwargs["binrepr"]
            self.year = int.from_bytes(b[0:2], byteorder="big")
            self.month = b[2]
            self.day = b[3]
            self.hour = b[4]
            self.minute = b[5]
            self.replicate = int.from_bytes(b[6:], byteorder="big")
        elif "time" in kwargs:
            self.year, self.month, self.day, self.hour, self.minute, _ = kwargs["time"]
            self.replicate = kwargs["replicate"]
        else:
            raise TypeError("ceci est une erreur")

    def binrepr(self):
        return (
            self.year.to_bytes(2, byteorder="big")
            + self.month.to_bytes(1, byteorder="big")
            + self.day.to_bytes(1, byteorder="big")
            + self.hour.to_bytes(1, byteorder="big")
            + self.minute.to_bytes(1, byteorder="big")
            + self.replicate.to_bytes(4, byteorder="big")
        )

    def __lt__(self, oth):
        if self.year != oth.year:
            return self.year < oth.year
        if self.month != oth.month:
            return self.month < oth.month
        if self.day != oth.day:
            return self.day < oth.day
        if self.hour != oth.hour:
            return self.hour < oth.hour
        if self.minute != oth.minute:
            return self.minute < oth.minute
        return self.replicate < oth.replicate

    def __eq__(self, oth):
        return (
            self.year == oth.year
            and self.month == oth.month
            and self.day == oth.day
            and self.hour == oth.hour
            and self.minute == oth.minute
            and self.replicate == oth.replicate
        )

    def __le__(self, oth):
        return self < oth or self == oth


def writelmdb_bytime(csvfilename, lmdbfilename):
    env = lmdb.open(lmdbfilename, map_size=2 ** 30)
    inserted = 0
    with env.begin(write=True) as txn:
        for data in loadata(csvfilename):
            proposed_key = TimeKey(time=data["starttime"], replicate=0)
            while txn.get(proposed_key.binrepr()) is not None:
                proposed_key.replicate += 1
            txn.put(proposed_key.binrepr(), json.dumps(data).encode())
            inserted += 1
    return inserted
