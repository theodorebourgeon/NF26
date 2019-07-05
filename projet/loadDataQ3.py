import cassandra
import cassandra.cluster
import re
import time
import csv
from tqdm import tqdm
from datetime import datetime

KEYSPACE = 'thbourge_td3'
TABLE = 'projectq3'
FILE = './data/asos.txt'

cluster = cassandra.cluster.Cluster()
session = cluster.connect(KEYSPACE)

session.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
            station text,
            year int, 
            month int, 
            day int, 
            hour int, 
            minute int, 
            lat float ,
            lon float ,
            tmpf float,
            relh float,
            sknt int,
            drct float, 
            alti float,
            feel float,
            PRIMARY KEY ((year, month, day ),station, hour, minute)
        )
    """)

SQLREQUEST = session.prepare(
    f"""INSERT INTO {KEYSPACE}.{TABLE} (station, year, month, day, hour, minute, lat,lon,tmpf, relh, sknt, drct, alti, feel)
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")

with open(FILE) as csvfile:
    dateparser = re.compile(r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)")
    dial = csv.excel
    dial.delimiter = ','
    reader = csv.DictReader(csvfile, dialect=dial)

    batch = cassandra.query.BatchStatement()
    COUNT = 0
    BATCH_NB = 0
    BATCH_SIZE = 50

    for row in tqdm(reader):
        match_date = dateparser.match(row["valid"])
        if not match_date:
            continue
        date_dict = match_date.groupdict()
        station = row["station"] if row["station"] != "M" else None
        valid = (
            int(date_dict["year"]),
            int(date_dict["month"]),
            int(date_dict["day"]),
            int(date_dict["hour"]),
            int(date_dict["minute"]),
            )
        lon = float(row['lon']) if row['lon'] != "M" else None
        lat = float(row['lat']) if row['lat'] != "M" else None
        tmpf = float(row["tmpf"]) if row["tmpf"] != "M" else None
        relh = float(row["relh"]) if row["relh"] != "M" else None
        sknt = int(float(row["sknt"])) if row["sknt"] != "M" else None
        drct = float(row["drct"]) if row["drct"] != "M" else None
        alti = float(row["alti"]) if row["alti"] != "M" else None
        feel = float(row["feel"]) if row["feel"] != "M" else None

        if COUNT<BATCH_SIZE:
            batch.add(SQLREQUEST, (station, valid[0], valid[1], valid[2], valid[3], valid[4], lat, lon, tmpf, relh, sknt, drct, alti, feel))
            COUNT += 1
        else:
            COUNT = 0
            session.execute(batch) 
            BATCH_NB += 1
            batch = cassandra.query.BatchStatement()
    if COUNT > 0:
        session.execute(batch)
        BATCH_NB += 1

print(f"""{BATCH_NB} batchs effectué""")
