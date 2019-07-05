import manip as manip
import pandas as pd

# Modélisons des données

# print(manip.head("./data/201307-201402-citibike-tripdata"))

# Data 
# {'tripduration': '1010', 
# 'starttime': '2013-09-01 00:00:02', 
# 'stoptime': '2013-09-01 00:16:52', 

# 'start station id': '254', 
# 'start station name': 'W 11 St & 6 Ave', 
# 'start station latitude': '40.73532427', 
# 'start station longitude': '-73.99800419', 

# 'end station id': '147', 
# 'end station name': 'Greenwich St & Warren St', 
# 'end station latitude': '40.71542197', 
# 'end station longitude': '-74.01121978', 

# 'bikeid': '15014', 
# 'usertype': 'Subscriber', 
# 'birth year': '1974', 
# 'gender': '1'}

##• Quels sont les trajets partant de telle zone, arrivant dans telle zone ?
## Indexation géographique
## [ [Fct(long_Dep,lat_Dep)] [Fct(long_Arr,lat_Arr)]  ... ]

def searchTripStationId(startId, endId):
    search = []
    for l in manip.getdata("./data/201307-201402-citibike-tripdata"):
        if(l["start station id"] == str(startId) and l["end station id"] == str(endId)):
            search.append(l)
    return search

# print(searchTripStationId(404,445))

##• Quels sont les trajet qui partent tel jour à telle heure ?
## [ [annee,mois] [jours] [heure] ...... ] 

def searchTripDate(startDate):
    search = []
    for l in manip.getdata("./data/201307-201402-citibike-tripdata"):
        if(l["starttime"] == startDate):
            search.append(l)
    return search

# print(searchTripDate('2013-08-30 15:09:41'))

##• Quelles sont les zones les plus actives le lundi ?
## [ [Fct(long_(Dep ou Arr),lat_(Dep ou Arr)] [jours] ... ]


# Stockage clé/valeur

## Modéliser un ou des magasins clef valeur pour pouvoir répondre à ces questions en un nombre minimal de requètes

## Implémenter ces magasins clef/valeurs avec lmdb

### EXEMPLE

#### LMDB 
# Ouvrir transaction dans un with (associé à un contexte pour qu'elles se ferment)
# la base est persistante 
# import lmdb

# env = lmdb.open('./database')
# with env.begin(write=True) as txn:
#     txn.put(b'key',b'value')

# #b'...' pour preciser qu'il s'agit d'un tableau d'octet sous code ascii
# with env.begin(write=True) as txn:
#     print(txn.get(b'key'))

# env.close()

#### JSON
# import json 
# o = [{'a':1,'b':0},2]
# o
# > [{'a': 1, 'b': 0}, 2]
# json.dumps(o)
# >'[{"a": 1, "b": 0}, 2]'
# json.dumps(o).encode()
# > b'[{"a": 1, "b": 0}, 2]'
# json.loads(json.dumps(o).encode().decode())
# > [{'a': 1, 'b': 0}, 2]

##### PICKLE
# import pickle
# o = [{'a':1,'b':0},2]
# o
# > [{'a': 1, 'b': 0}, 2]
# pickle.dumps(o)
# >b'\x80\x03]q\x00(}q\x01(X\x01\x00\x00\x00aq\x02K\x01X\x01\x00\x00\x00bq\x03K\x00uK\x02e.'


## Charger les données dans iceux


# Stockage orienté colonne

