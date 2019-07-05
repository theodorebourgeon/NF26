import cassandra
import cassandra.cluster
import re
import time
import csv
from tqdm import tqdm
from datetime import datetime

KEYSPACE = 'thbourge_td3'
TABLE = 'projectQ12'
FILE = './data/asos.txt'

cluster = cassandra.cluster.Cluster()
session = cluster.connect(KEYSPACE)

session.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
            lon_t float,
            lat_t float,
            station text,
            lon float,
            lat float,
            year int, 
            month int, 
            day int, 
            hour int, 
            minute int, 
            tmpf float,
            relh float,
            sknt int,
            drct float, 
            alti float,
            feel float,
            PRIMARY KEY ((lon_t, lat_t), year, month, day, hour, minute, lon, lat)
        )
    """)

SQLREQUEST = session.prepare(
    f"""INSERT INTO {KEYSPACE}.{TABLE} (lon_t, lat_t, year, month, day, hour, minute, lon, lat, station, tmpf, relh, sknt, drct, alti, feel)
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")

with open(FILE) as csvfile:
    dateparser = re.compile(r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)")
    dial = csv.excel
    dial.delimiter = ','
    reader = csv.DictReader(csvfile, dialect=dial)

    batch = cassandra.query.BatchStatement()
    COUNT = 0
    BATCH_NB = 0
    BATCH_SIZE = 200

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
        if row["lon"] != "M":
            lon = float(row["lon"])  
        else:
            continue
        if row["lat"] != "M":
            lat = float(row["lat"])  
        else:
             continue
        lon_t = int(lon)
        lat_t = int(lat)
        tmpf = float(row["tmpf"]) if row["tmpf"] != "M" else None
        relh = float(row["relh"]) if row["relh"] != "M" else None
        sknt = int(float(row["sknt"])) if row["sknt"] != "M" else None
        drct = float(row["drct"]) if row["drct"] != "M" else None
        alti = float(row["alti"]) if row["alti"] != "M" else None
        feel = float(row["feel"]) if row["feel"] != "M" else None

        if COUNT<BATCH_SIZE:
            batch.add(SQLREQUEST, (lon_t, lat_t, valid[0], valid[1], valid[2], valid[3], valid[4], lon , lat, station, tmpf, relh, sknt, drct, alti, feel))
            COUNT += 1
        else:
            COUNT = 0
            session.execute(batch) 
            BATCH_NB += 1
            batch = cassandra.query.BatchStatement()
    if COUNT > 0:
        session.execute(batch)
        BATCH_NB += 1

print(f"""{BATCH_NB} effectué""")
