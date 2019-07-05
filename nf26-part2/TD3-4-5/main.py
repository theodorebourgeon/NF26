import KeySpaceCreation
import Table
import loadData
import os
import json

# print(KeySpaceCreation.createKeySpace("thbourge_td3"))
# table = Table.table("thbourge_td3")
# table.create("query3")

# nbrow = 0
# for row in loadData.loadataDirectory("./data/201307-201402-citibike-tripdata"):
#     table.insert(row)
#     nbrow = nbrow + 1
#     print(nbrow)

# year = 2013
# month = 7 
# day = 1
# hour = 0
# for row in table.getTravel(year,month,day,hour):
#     print(row.json)

# TD stockage colonne, calculs et représentations

table_by_day = Table.table("thbourge_td3")
table_by_day.create_by_day("by_day")
# for row in loadData.loadataDirectory("./data/201307-201402-citibike-tripdata"):
#     table_by_day.insert(row)

# • Pour un jour donné, calculez les statistiques élémentaires sur les temps de trajet
table_by_day.getDurationStat_by_day(2013,7,1)

# • Pour un jour donné, calculez la distribution des temps de trajet
# table_by_day.getDurationHist_by_day(2013,8,1)
# • Pour un jour donné, calculez les statistiques élémentaires des distances
# • Pour un jour donné, calculez la distribution des temps de trajet
#
# Répétez le processus pour tous les jours sur une période de temps, produisez
# une analyse finie à l’utilisateur. Un pdf ou une page html doit être générée


# K-mean
# On veut juste le centre des classes pour tracer le graphe de voronoid 

# Initialiser centroides
# Procedure update_centroide(centre):
#     vecteur de somme (0,0,0,0) -> dim des données du kmeans
#     count (0,0,0,0,0,0)  -> dim N 
#     Pour les data dans données : 
#         Faire :
#             c <- Classe du points (en utilisant centroides)
#             somme[c] += data 
#             count[c] += 1 
#         nouveau_centroides  <- [somme[c]/count[c]]

# Procedure à répéter jusqu'à convergeance

# TD SPARK