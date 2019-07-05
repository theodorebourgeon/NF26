
# Configuratins related to Cassandra connector & Cluster
import os
import socket
import pandas as pd
from pyspark import SparkContext  # 导入模块
from pyspark.sql import SQLContext
from pyspark.sql.functions import *



os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

# Creating PySpark Context
socket.gethostbyname('localhost')
sc = SparkContext("local", "asos app")

# Creating PySpark SQL Context
sqlContext = SQLContext(sc)

KEYSPACE = 'projet_nf26'
TABLE = 'projectq12'

# Loads and returns data frame for a table including key space given
def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keys_space_name) \
        .load()
    return table_df

    # Loading table data frames
data = load_and_get_table_df(KEYSPACE, TABLE)




def getElementaryStatyStationbyYearForMonth(station,year):
    meanMonth = []
    meanFeelMonth = []
    minMonth = []
    maxMonth = []
    date = []

    exprs = [count("tmpf"), mean("tmpf"), min("tmpf"), max("tmpf"),mean("feel")]
    data_s_y = data \
            .groupBy("station", "year", "month") \
            .agg(*exprs) \
            .filter((col("station") == station) & (col("year") == year)) \
            .orderBy("month")

    nbMonth = data_s_y.select(count("month")).collect()[0][0]

    for month in range(0, nbMonth):
        meanMonth.append(data_s_y.collect()[month][4])
        meanFeelMonth.append(data_s_y.collect()[month][7])
        minMonth.append(data_s_y.collect()[month][5])
        maxMonth.append(data_s_y.collect()[month][6])
        date.append(data_s_y.collect()[month][2])

    return (meanMonth,meanFeelMonth, minMonth, maxMonth, date)

def getElementaryStatyStationbyYear(station,year):

    exprs = [count("tmpf"),mean("tmpf"), min("tmpf"), max("tmpf")]
    data_s_y = data\
        .groupBy("station", "year")\
        .agg(*exprs)\
        .filter((col("station") == station) & (col("year") == year))\
        .collect()[0]

    return (data_s_y[2], data_s_y[3], data_s_y[4], data_s_y[5])


def getElementaryStatyStation(station):
    SumMeanTmpf = 0
    meanTmpf = 0
    minTmpf = 1000
    maxTmpf =0
    size=0
    count = 0
    for i in range(2001,2011):
        stat = getElementaryStatyStationbyYear(station, i)
        if stat[0] != 0:
            size += stat[0]
            SumMeanTmpf += stat[1]
            minTmpf = (stat[2], minTmpf)[minTmpf < stat[2]]
            maxTmpf = (stat[3], maxTmpf)[maxTmpf > stat[3]]
            count+=1
    if count !=0:
        meanTmpf = SumMeanTmpf/count
    return (size, meanTmpf, minTmpf, maxTmpf)


def getElementaryStatyStationForMonth(station):
    listMean = []
    listMeanFeel = []
    listMeanMin = []
    listMeanMax = []
    listDate = []
    for i in range(2001,2011):
        # (meanMonth,meanFeelMonth, minMonth, maxMonth, date)
        stat = getElementaryStatyStationbyYearForMonth(station, i)
        listMean.extend(stat[0])
        listMeanFeel.extend(stat[1])
        listMeanMin.extend(stat[2])
        listMeanMax.extend(stat[3])
        date = [f"""{i}-{m}""" for m in stat[4]]
        listDate.extend(date)
    return (listMean, listMeanFeel, listMeanMin, listMeanMax, listDate)


def getGraphStatyStationEvolution(station):
    import matplotlib.pyplot as plt
    tmpf =[]
    year = []
    for i in range(2001,2011):
        stat = getElementaryStatyStationbyYear(station, i)
        if stat[0] != 0:
            tmpf.append(stat[1])
            year.append(str(i))
    # style
    plt.style.use('seaborn-darkgrid')
    plt.plot(year, tmpf)
    plt.title(f"""Evolution générale de la température entre 2001 et 2010""")
    plt.ylabel('Température en F')
    plt.xlabel(f"""Années""")
    plt.savefig(f"""./images/{station}-evolution-tmpf.png""")
    plt.close()

def getGraphStatyStationbyYear(station,year):
    import matplotlib.pyplot as plt

    data_s_y = data \
        .filter((col("station") == station) & (col("year") == year)) \
        .select("tmpf")

    tmpfs = pd.DataFrame(data_s_y.collect())
    tmpfs.columns = ['tmpf']
    tmpf = tmpfs['tmpf'].tolist()

    # style
    plt.style.use('seaborn-darkgrid')
    plt.plot(tmpf)
    plt.title(f"""Evolution de la température au cours de l'année {year}""")
    plt.ylabel('Température en F')
    plt.xlabel(f"""Relevé de l'année {year}""")
    plt.savefig(f"""./images/{station}-{year}.png""")
    plt.close()


def getGraphEvolutionByStationByYear(station,year):
    import matplotlib.pyplot as plt
    # Get stat by month (meanMonth,meanFeelMonth, minMonth, maxMonth, date)
    stat = getElementaryStatyStationbyYearForMonth(station, year)
    # style
    plt.style.use('seaborn-darkgrid')
    # multiple line plot
    plt.plot(stat[4], stat[0], marker='o', markerfacecolor='grey', markersize=4, color='grey', linewidth=2, label="Moyenne réelle")
    plt.plot(stat[4], stat[1], marker='', color="green", linewidth=1, label="Moyenne ressentie")
    plt.plot(stat[4], stat[2], marker='', color="blue", linewidth=1, label="Minimale")
    plt.plot(stat[4], stat[3], marker='', color="red", linewidth=1, label="Maximale")
    # Add legend
    plt.legend()
    plt.title(f"""Evolution moyenne des températures au cours de l'année {year}""")
    plt.ylabel('Température en F')
    plt.xlabel(f"""Mois de l'année {year}""")
    plt.savefig(f"""./images/{station}-{year}-sumup.png""")
    plt.close()

def getGraphEvolutionByStation(station):
    import matplotlib.pyplot as plt
    # Get stat by month (meanMonth,meanFeelMonth, minMonth, maxMonth, date)
    stat = getElementaryStatyStationForMonth(station)
    # style
    plt.style.use('seaborn-darkgrid')
    # multiple line plot
    plt.plot(stat[4], stat[0], marker='o', markerfacecolor='grey', markersize=4, color='grey', linewidth=2, label="Moyenne réelle")
    plt.plot(stat[4], stat[1], marker='', color="green", linewidth=1, label="Moyenne ressentie")
    plt.plot(stat[4], stat[2], marker='', color="blue", linewidth=1, label="Minimale")
    plt.plot(stat[4], stat[3], marker='', color="red", linewidth=1, label="Maximale")
    # Add legend
    plt.legend()
    plt.xticks(rotation='vertical')
    plt.margins(0,0.25)
    # Tweak spacing to prevent clipping of tick-labels
    plt.subplots_adjust(bottom=0.25)
    plt.title(f"""Evolution moyenne des températures de 2001 à 2010""")
    plt.ylabel('Température en F')
    plt.xlabel(f"""Temps""")
    plt.savefig(f"""./images/{station}-sumup.png""")
    plt.close()

def getGraphStatSumUpByStationByYear(station,year):
    import matplotlib.pyplot as plt
    # Get stat by month (meanMonth,meanFeelMonth, minMonth, maxMonth, date)
    stat = getElementaryStatyStationbyYearForMonth(station, year)
    # style
    plt.style.use('seaborn-darkgrid')
    # multiple line plot
    plt.plot(stat[4], stat[0], marker='o', markerfacecolor='grey', markersize=4, color='grey', linewidth=2, label="Moyenne réelle")
    plt.plot(stat[4], stat[1], marker='', color="green", linewidth=1, label="Moyenne ressentie")
    plt.plot(stat[4], stat[2], marker='', color="blue", linewidth=1, label="Minimale")
    plt.plot(stat[4], stat[3], marker='', color="red", linewidth=1, label="Maximale")
    # Add legend
    plt.legend()
    plt.title(f"""Evolution moyenne des températures au cours de l'année {year}""")
    plt.ylabel('Température en F')
    plt.xlabel(f"""Mois de l'année {year}""")
    plt.savefig(f"""./images/{station}-{year}-sumup.png""")
    plt.close()


def question1(Station, year=None):
    if year == None:
        data = getElementaryStatyStation(Station)
        getGraphStatyStationEvolution(Station)
        getGraphEvolutionByStation(Station)
        print(f"""Il y a eu au total {data[0]} relevé de la station {Station}""")
        print(f"""La température moyenne relevée est de {data[1]} F""")
        print(f"""La température maximale relevée est de {data[2]} F""")
        print(f"""La température minimale relevée est de {data[3]} F""")

    else :
        data = getElementaryStatyStationbyYear(Station, year)
        print(f"""Il y a eu {data[0]} relevé de la station {Station} au cours de l'année {year}""")
        print(f"""La température moyenne relevée est de {data[1]} F""")
        print(f"""La température maximale relevée est de {data[2]} F""")
        print(f"""La température minimale relevée est de {data[3]} F""")
        getGraphStatyStationbyYear(Station,year)
        getGraphEvolutionByStationByYear(Station,year)
        getGraphStatSumUpByStationByYear(Station,year)
    return 0

#question1("LFSX")
# question1("LFRT", 2001)