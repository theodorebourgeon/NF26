# clustering dataset
# determine k using elbow method
# Configuratins related to Cassandra connector & Cluster
import os
import re
import socket

import folium
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

'''
********************************************************************************
Partie initialisation de l'environnement
********************************************************************************
'''

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

# Creating PySpark Context
socket.gethostbyname('localhost')
sc = SparkContext("local", "asos app")

# Creating PySpark SQL Context
sqlContext = SQLContext(sc)

KEYSPACE = 'thbourge_td3'
TABLE = 'projectq3'

'''
********************************************************************************
Partie Definition de fonction outil
********************************************************************************
'''


# Loads and returns data frame for a table including key space given
def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keys_space_name) \
        .load()
    return table_df


# Chargement de donnees dans un periode donnee
# La fonctionne regroupe les donnee par station
# - calcule pour chaque station la tmpf, drct, sknt et relh moyenne
# - scale les donnees pour apprentissage
# - re-attache les lat, lon initial pour afficher correctement des point sur map
# - retourne 2 daaframe, 1 contient les donnees centrees reduites, l'autre contient donnees initiales
def get_pandas_df(starttime, stoptime):
    dateparser = re.compile(r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)")
    match_startdate = dateparser.match(starttime)
    match_stopdate = dateparser.match(stoptime)
    if not match_startdate or not match_stopdate:
        print("Rentrez une date valide YYYY-MM-DD")
        return
    date_dict = match_startdate.groupdict()
    year_debut = int(date_dict["year"])
    month_debut = int(date_dict["month"])
    day_debut = int(date_dict["day"])

    date_dict = match_startdate.groupdict()
    year_fin = int(date_dict["year"])
    month_fin = int(date_dict["month"])
    day_fin = int(date_dict["day"])

    data = load_and_get_table_df(KEYSPACE, TABLE)
    exprs = [mean("lat"), mean("lon"), mean("tmpf"), mean("drct"), mean("sknt"), mean("relh")]

    data_bis = data \
        .filter((col("year") >= year_debut) & (col("year") <= year_fin) & (col("month") >= month_debut) & (
            col("month") <= month_fin) & (col("day") >= day_debut) & (col("day") <= day_fin)) \
        .groupBy("station") \
        .agg(*exprs) \
        .select("station", "avg(lat)", "avg(lon)", "avg(tmpf)", "avg(drct)", "avg(sknt)", "avg(relh)")

    # data_bis.show()

    df_pd = pd.DataFrame(data_bis.collect())
    df_pd = df_pd.fillna(df_pd.mean())

    categorical_features = ['station']
    continuous_features = ["lat", "lon", "tmpf", "drct", "sknt", "relh"]

    df_pd.columns = ["station", "lat", "lon", "tmpf", "drct", "sknt", "relh"]

    df_pd_tr = df_pd[continuous_features]
    df_pd_tr = df_pd_tr.fillna(df_pd_tr.mean())

    mms = MinMaxScaler()
    df_pd_tr[["tmpf", "drct", "sknt", "relh"]] = mms.fit_transform(df_pd_tr[["tmpf", "drct", "sknt", "relh"]])
    return df_pd_tr, df_pd


# la focntion qui effectue l'algorithme Kmeans
# retourne sur un periode et k donne, la datafrme des sation avec leur numeros de cluster.
def get_cluster(starttime, stoptime, k):
    df = get_pandas_df(starttime, stoptime)

    df_origin = df[1]
    df = df[0]

    kmeans = KMeans(n_clusters=k)
    clusters_knn = kmeans.fit_predict(df)
    df_origin["label_kmeans"] = clusters_knn

    kmeans = kmeans.fit(df)
    centroids = kmeans.cluster_centers_
    # print(centroids)

    plt.scatter(df['lat'], df['lon'], c=kmeans.labels_.astype(float), s=50, alpha=0.5)
    plt.scatter(centroids[:, 0], centroids[:, 1], c='red', s=50)
    plt.show()

    return df_origin, centroids


def getMapStationInformationbyFullDate(df, k):
    m = folium.Map(location=[47.029895, 2.440967], zoom_start=6)

    for row in df.itertuples(index=True, name='Pandas'):
        # getattr(row, "c1"), getattr(row, "c2")
        html = f"""
                        <b> informations de periode</b></br></br>
                        <ul>
                            <li>Temperature réelle: {getattr(row, "tmpf")} F</li>
                            <li>Force du vent : {getattr(row, "sknt")} noeuds</li>
                            <li>Direction du vent : {getattr(row, "drct")} degrée</li>
                            <li>Humidité de l'air : {getattr(row, "relh")} %</li>
                        </ul>
                    """
        popupHtml = folium.Html(html, script=True)
        popup = folium.Popup(popupHtml, max_width=300, min_width=300)

        marker_colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred',
                         'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue',
                         'darkpurple', 'white', 'pink', 'lightblue', 'lightgreen',
                         'gray', 'black', 'lightgray']

        tooltip = getattr(row, "station")

        folium.Marker([getattr(row, "lat"), getattr(row, "lon")], popup=popup, tooltip=tooltip,
                      icon=folium.Icon(color=marker_colors[getattr(row, "label_kmeans")], icon='info-sign')).add_to(m)
    m.save(f"""./images/map_q3.html""")


def question3(starttime, stoptime, k):
    clusters = get_cluster(starttime, stoptime, k)
    print("\nKmeans avec succes, ", k, "clusters on totals")
    getMapStationInformationbyFullDate(clusters[0], k)
    print("\nResultat enregistre dans ./images/map_q3.html")


# starttime = "2001-1-1"
# stoptime = "2001-10-31"
# k = 5

# question3(starttime, stoptime, k)
