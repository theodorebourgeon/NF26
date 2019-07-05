import cassandra
import cassandra.cluster

KEYSPACE = 'thbourge_td3'
TABLE = 'projectq12'

cluster = cassandra.cluster.Cluster()
session = cluster.connect(KEYSPACE)


def getDist(point1, point2):
    """
    Renvoie la distance en km entre deux points représenté par leur coordonnées géographiques
    Format des points = (lon,lat) en degrée
    """
    from math import sin, cos, sqrt, atan2, radians
    R = 6373.0
    
    lat1 = radians(point1[0])
    lon1 = radians(point1[1])
    lat2 = radians(point2[0])
    lon2 = radians(point2[1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

def getclosest(point1, point2, point3):
    """
    Return the closest point to point1
    Format of points = (lon, lat)
    """
    dist1 = getDist(point1, point2)
    dist2 = getDist(point1, point3)

    if (dist1<dist2):
        return 1
    else:
        return 2


def getClosestSationByYearSingleBlock(lon,lat, year):
    """
    Return the closest Station for the zone of the point choosed for one year
    """
    lon_t = int(lon)
    lat_t = int(lat)
    rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where lon_t={lon_t} AND  lat_t={lat_t} AND year={year}""")
    for row in rows: 
        row0 = None
        row1 = row
        row2 = row
        point0 = (0,0)
        point1 = (row.lon, row.lat)
        point2 = (row.lon, row.lat)
        res = getclosest(point0,point1, point2)
        if res == 1:
            row0 = row1
        else: 
            row0 = row2
    return row0.station

def getClosestSationSingleBlock(lon,lat):
    """
    Return the closest Station for the zone of the point choosed 
    """
    lon_t = int(lon)
    lat_t = int(lat)
    for year in range(2001,2011):
        rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where lon_t={lon_t} AND  lat_t={lat_t} AND year={year}""")
        row0 = None
        for row in rows: 
            row1 = row
            row2 = row
            point0 = (0,0)
            point1 = (row.lon, row.lat)
            point2 = (row.lon, row.lat)
            res = getclosest(point0,point1, point2)
            if res == 1:
                row0 = row1
            else: 
                row0 = row2
    return (row0.station if row0!=None else None)


def getClosestSationByYearMultiBlock(lon,lat, year):
    """
    Return the closest Station for the zone of the point choosed for one year
    """
    lon_t = int(lon)
    lat_t = int(lat)
    row0 = None
    for lon_m in (lon_t - 1,lon_t, lon_t + 1):
        for lat_m in (lat_t - 1,lat_t, lat_t + 1):
            rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where lon_t={lon_m} AND  lat_t={lat_m} AND year={year}""")
            for row in rows: 
                row1 = row
                row2 = row
                point0 = (0,0)
                point1 = (row.lon, row.lat)
                point2 = (row.lon, row.lat)
                res = getclosest(point0,point1, point2)
                if res == 1:
                    row0 = row1
                else: 
                    row0 = row2
    return row0.station

def getClosestSationMultiBlock(lon,lat):
    """
    Return the closest Station for the zone of the point choosed 
    """
    lon_t = int(lon)
    lat_t = int(lat)
    row0 = None
    for lon_m in (lon_t - 1,lon_t, lon_t + 1):
        for lat_m in (lat_t - 1,lat_t, lat_t + 1):
            for year in range(2001,2011):
                rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where lon_t={lon_m} AND  lat_t={lat_m} AND year={year}""")
                for row in rows: 
                    row1 = row
                    row2 = row
                    point0 = (0,0)
                    point1 = (row.lon, row.lat)
                    point2 = (row.lon, row.lat)
                    res = getclosest(point0,point1, point2)
                    if res == 1:
                        row0 = row1
                    else: 
                        row0 = row2
    return (row0.station if row0!=None else None)

def question12(lon,lat,year=None, version="single"):
    if version =="single":
        import processingQ1
        if year == None:
            Station = getClosestSationSingleBlock(lon,lat)
            if Station != None: 
                data = processingQ1.getElementaryStatyStation(Station)
                processingQ1.getGraphStatyStationEvolution(Station)
                processingQ1.getGraphEvolutionByStation(Station)
                print(f"""Il y a eu au total {data[0]} relevé de la station {Station}""")
                print(f"""La température moyenne relevée est de {data[1]} F""") 
                print(f"""La température maximale relevée est de {data[3]} F""") 
                print(f"""La température minimale relevée est de {data[2]} F""")
            else: 
                print("Ce point ne possède pas de donnée")
            
        else : 
            Station = getClosestSationByYearSingleBlock(lon,lat, year)
            if Station != None: 
                data = processingQ1.getElementaryStatyStationbyYear(Station, year)
                print(f"""Il y a eu {data[0]} relevé de la station {Station} au cours de l'année {year}""")
                print(f"""La température moyenne relevée est de {data[1]} F""") 
                print(f"""La température maximale relevée est de {data[3]} F""") 
                print(f"""La température minimale relevée est de {data[2]} F""")
                processingQ1.getGraphStatyStationbyYear(Station,year)
                processingQ1.getGraphEvolutionByStationByYear(Station,year)
                processingQ1.getGraphStatSumUpByStationByYear(Station,year)
            else: 
                print("Ce point ne possède pas de donnée")
    elif version =="multi":
        import processingQ1
        if year == None:
            Station = getClosestSationMultiBlock(lon,lat)
            if Station != None: 
                data = processingQ1.getElementaryStatyStation(Station)
                processingQ1.getGraphStatyStationEvolution(Station)
                processingQ1.getGraphEvolutionByStation(Station)
                print(f"""Il y a eu au total {data[0]} relevé de la station {Station}""")
                print(f"""La température moyenne relevée est de {data[1]} F""") 
                print(f"""La température maximale relevée est de {data[3]} F""") 
                print(f"""La température minimale relevée est de {data[2]} F""")
            else: 
                print("Ce point ne possède pas de donnée")
            
        else : 
            Station = getClosestSationByYearMultiBlock(lon,lat, year)
            if Station != None: 
                data = processingQ1.getElementaryStatyStationbyYear(Station, year)
                print(f"""Il y a eu {data[0]} relevé de la station {Station} au cours de l'année {year}""")
                print(f"""La température moyenne relevée est de {data[1]} F""") 
                print(f"""La température maximale relevée est de {data[3]} F""") 
                print(f"""La température minimale relevée est de {data[2]} F""")
                processingQ1.getGraphStatyStationbyYear(Station,year)
                processingQ1.getGraphEvolutionByStationByYear(Station,year)
                processingQ1.getGraphStatSumUpByStationByYear(Station,year)
            else: 
                print("Ce point ne possède pas de donnée")
    else: 
        print("Cette version n'exsite pas : Utilisez la version \"single\" ou \"multi\"")

# question12(4.786645,48.267885, version="single")
# question12(4.786645,48.267885, 2001, version="single")
# question12(4.786645,48.267885, version="multi")
# question12(4.786645,48.267885, 2001, version="multi")