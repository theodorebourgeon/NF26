import cassandra
import folium
import re
import cassandra.cluster

KEYSPACE = 'thbourge_td3'
TABLE = 'projectq2'

cluster = cassandra.cluster.Cluster()
session = cluster.connect(KEYSPACE)

def getMapStationInformationbyFullDate(date):
    dateparser = re.compile(r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)")
    match_date = dateparser.match(date)
    if not match_date:
        print("Rentrez une date valide YYYY-MM-DD HH:MM")
        return 
    date_dict = match_date.groupdict()
    rows = session.execute(f"""SELECT * FROM {KEYSPACE}.{TABLE} where year={int(date_dict['year'])} AND month={int(date_dict['month'])} AND day={int(date_dict['day'])} AND hour={int(date_dict['hour'])} AND minute={int(date_dict['minute'])}""")
    m = folium.Map(location=[47.029895, 2.440967], zoom_start=6)
    if rows:
        for row in rows:
            html=f"""
                <b>{row.station} informations le {date}</b></br></br>
                <ul>
                    <li>Temperature réelle: {round(row.tmpf,1)} F</li>
                    <li>Temperature ressentie: {round(row.feel,1)} F</li>
                    <li>Pression atmosphérique : {round(row.alti,1)} inches</li>
                    <li>Force du vent : {round(row.sknt,1)} noeuds</li>
                    <li>Direction du vent : {round(row.drct,1)} degrée</li>
                    <li>Humidité de l'air : {round(row.relh,1)} %</li>
                </ul>
            """
            popupHtml = folium.Html(html, script=True)
            popup = folium.Popup(popupHtml,max_width=300,min_width=300)
            
            tooltip = row.station
            folium.Marker([row.lat, row.lon], popup=popup, tooltip=tooltip).add_to(m)
        m.save(f"""./images/map-info-{date}.html""")

        

def question2(date):
    getMapStationInformationbyFullDate(date)

# question2("2001-01-01 00:30")