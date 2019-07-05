import cassandra
import cassandra.cluster

KEYSPACE = 'thbourge_td3'
TABLE = 'projectq1'

cluster = cassandra.cluster.Cluster()
session = cluster.connect(KEYSPACE)


def getElementaryStatyStationbyYearForMonth(station,year):
    meanMonth = []
    meanFeelMonth = []
    minMonth = []
    maxMonth = []
    date = []
    for month in range(1,13):
        rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where station='{station}' AND year={year} AND month={month}""")
        size = 0
        sumTmpf = 0
        sumFeel = 0
        minTmpf = 1000
        maxTmpf = 0
        meanTmpf =0 
        meanFeel = 0
        if rows : 
            for row in rows:
                if row.tmpf != None and row.feel != None:
                    size += 1
                    sumTmpf +=  row.tmpf
                    sumFeel += row.feel
                    if row.tmpf < minTmpf:
                        minTmpf = row.tmpf
                    if row.tmpf > maxTmpf:
                        maxTmpf = row.tmpf
            if size!=0:
                meanTmpf = sumTmpf/size
                meanFeel = sumFeel/size
            meanMonth.append(meanTmpf)
            meanFeelMonth.append(meanFeel)
            minMonth.append(minTmpf)  
            maxMonth.append(maxTmpf)
            date.append(month)
    return (meanMonth,meanFeelMonth, minMonth, maxMonth, date)

def getElementaryStatyStationbyYear(station,year):
    rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where station='{station}' AND year={year}""")
    size = 0
    sumTmpf = 0
    minTmpf = 1000
    maxTmpf = 0
    meanTmpf =0 
    if rows : 
        for row in rows:
            if row.tmpf != None:
                size += 1
                sumTmpf +=  row.tmpf
                if row.tmpf < minTmpf:
                    minTmpf = row.tmpf
                if row.tmpf > maxTmpf:
                    maxTmpf = row.tmpf
        if size!=0:
            meanTmpf = sumTmpf/size     
    return (size, meanTmpf, minTmpf, maxTmpf)

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
    rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where station='{station}' AND year={year}""")
    tmpf =[]
    if rows : 
        for row in rows:
            if row.tmpf != None:
                tmpf.append(row.tmpf)
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
        print(f"""La température maximale relevée est de {data[3]} F""") 
        print(f"""La température minimale relevée est de {data[2]} F""")
        
    else : 
        data = getElementaryStatyStationbyYear(Station, year)
        print(f"""Il y a eu {data[0]} relevé de la station {Station} au cours de l'année {year}""")
        print(f"""La température moyenne relevée est de {data[1]} F""") 
        print(f"""La température maximale relevée est de {data[3]} F""") 
        print(f"""La température minimale relevée est de {data[2]} F""")
        getGraphStatyStationbyYear(Station,year)
        getGraphEvolutionByStationByYear(Station,year)
        getGraphStatSumUpByStationByYear(Station,year)

# question1("LFRT")
# question1("LFRT", 2001)