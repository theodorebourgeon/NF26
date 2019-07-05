import cassandra
import cassandra.cluster
import exeption
import loadData

class table():
    def __init__(self,KEYSPACE):
        cluster = cassandra.cluster.Cluster()
        self.session = cluster.connect(KEYSPACE)

    def create(self, name):
        self.name = name
        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            tripduration int, 
            starttime_year int, 
            starttime_month int,
            starttime_day int, 
            starttime_hour int, 
            starttime_minute int,
            starttime_seconds float, 
            stopttime_year int, 
            stoptime_month int,
            stoptime_day int, 
            stoptime_hour int, 
            stoptime_minute int,
            stoptime_seconds float, 
            start_station_name text,
            start_station_lat float, 
            start_station_lon float,
            stop_station_name text, 
            stop_station_lat float,
            stop_station_lon float, 
            bikeid int, 
            usertype text,
            birth_year int, 
            gender int,
            PRIMARY KEY ((starttime_year, starttime_month), starttime_day, starttime_hour)
        )
        """)

    def create_by_day(self, name):
        self.name = name
        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            tripduration int, 
            starttime_year int, 
            starttime_month int,
            starttime_day int, 
            starttime_hour int, 
            starttime_minute int,
            starttime_seconds float, 
            stopttime_year int, 
            stoptime_month int,
            stoptime_day int, 
            stoptime_hour int, 
            stoptime_minute int,
            stoptime_seconds float, 
            start_station_name text,
            start_station_lat float, 
            start_station_lon float,
            stop_station_name text, 
            stop_station_lat float,
            stop_station_lon float, 
            bikeid int, 
            usertype text,
            birth_year int, 
            gender int,
            PRIMARY KEY ((starttime_year, starttime_month, starttime_day), starttime_hour)
        )
        """)

    def insert(self,row):
        prepared = self.session.prepare(f"""
            INSERT INTO {self.name} (
                starttime_year, 
                starttime_month,
                starttime_day, 
                starttime_hour,
                tripduration,
                starttime_minute,
                starttime_seconds, 
                stopttime_year, 
                stoptime_month,
                stoptime_day, 
                stoptime_hour, 
                stoptime_minute,
                stoptime_seconds, 
                start_station_name,
                start_station_lat, 
                start_station_lon,
                stop_station_name, 
                stop_station_lat,
                stop_station_lon, 
                bikeid, 
                usertype,
                birth_year, 
                gender
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """)
        try : 
            self.session.execute(prepared, (
                row["starttime"][0],
                row["starttime"][1],
                row["starttime"][2],
                row["starttime"][3],
                row["tripduration"],
                row["starttime"][4],
                row["starttime"][5],
                row["stoptime"][0],
                row["stoptime"][1],
                row["stoptime"][2],
                row["stoptime"][3],
                row["stoptime"][4],
                row["stoptime"][5],
                row["start station name"],
                row["start station latitude"],
                row["start station longitude"],
                row["stop station name"],
                row["stop station latitude"],
                row["stop station longitude"],
                row["bikeid"],
                row["usertype"],
                row["birth year"],
                row["gender"]
            ))
        except exeption.InsertError:
            print("Can't insert data, verify that the table is already well created.")
    
    def insertFromDirectory(self,directory):
        try: 
            for row in loadData.loadataDirectory(directory):
                self.insert(row)
        except exeption.InsertError:
            print(f"""Can't insert data from file : {directory}""")

    def readAll(self):
        try : 
            self.session.execute(f"""SELECT * FROM {self.name}""")
        except exeption.ReadError:
            print("Can't read all rows")
    
    def getTravel(self,year,month,day,hour):
        try : 
            return self.session.execute(f"""SELECT json * FROM {self.name} WHERE starttime_year={year} AND starttime_month={month} AND starttime_day={day} AND starttime_hour={hour};""")
        except exeption.ReadError:
            print("Can't read all rows")
    
    def getDurationStat_by_day(self,year,month,day):
        try : 
            rows = self.session.execute(f"""SELECT tripduration FROM {self.name} WHERE starttime_year={year} AND starttime_month={month} AND starttime_day={day};""")
            size = 0
            sumDuration = 0
            sumDurationSqured = 0 
            minD = 10000000000
            maxD = 0
            for row in rows:
                size += 1
                sumDuration +=  row.tripduration
                sumDurationSqured += row.tripduration ** 2
                if row.tripduration < minD:
                    minD = row.tripduration
                if row.tripduration > maxD:
                    maxD = row.tripduration

            if size!=0:
                meanDuration = sumDuration/size
                var = (sumDurationSqured/size - meanDuration**2)
            else: 
                meanDuration = 0
                var = 0

            h = int(meanDuration/3600)
            m = int((meanDuration - h*3600)/60)
            s = int((meanDuration- h*3600 - m*60)/60)
            print(f"""Il y a eu {size} trajets ce jour""")
            print(f"""Le trajet moyenne est de {h:2d}:{m:2d}:{s:2d}""") 
            print(f"""La variance est de {var}""") 
            print(f"""Le trajet max est de {maxD}s""") 
            print(f"""La trajet min est de {minD}s""") 
        except exeption.ReadError:
            print("Can't read all rows")

    def getDurationHist_by_day(self,year,month,day):
        try : 
            rows = self.session.execute(f"""SELECT tripduration FROM {self.name} WHERE starttime_year={year} AND starttime_month={month} AND starttime_day={day};""")
        except exeption.ReadError:
            print("Can't read all rows")