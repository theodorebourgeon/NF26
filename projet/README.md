# NF26_P19

Rapport : 
* https://fr.overleaf.com/1223342616bpvgzksvbvnh

# Intro 
Data France from 2001 to 2010 : https://mesonet.agron.iastate.edu/request/download.phtml?network=FR__ASOS 

Download data to your server : ```wget https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=LFAC&station=LFAQ&station=LFAT&station=LFBA&station=LFBC&station=LFBD&station=LFBE&station=LFBF&station=LFBG&station=LFBH&station=LFBI&station=LFBK&station=LFBL&station=LFBM&station=LFBN&station=LFBO&station=LFBP&station=LFBR&station=LFBS&station=LFBT&station=LFBU&station=LFBV&station=LFBX&station=LFBY&station=LFBZ&station=LFCC&station=LFCG&station=LFCI&station=LFCK&station=LFCR&station=LFDB&station=LFDH&station=LFFS&station=LFGA&station=LFGJ&station=LFHM&station=LFHP&station=LFIG&station=LFJL&station=LFJR&station=LFKB&station=LFKC&station=LFKF&station=LFKJ&station=LFKS&station=LFLA&station=LFLB&station=LFLC&station=LFLD&station=LFLL&station=LFLM&station=LFLN&station=LFLP&station=LFLQ&station=LFLS&station=LFLU&station=LFLV&station=LFLW&station=LFLX&station=LFLY&station=LFMA&station=LFMC&station=LFMD&station=LFME&station=LFMH&station=LFMI&station=LFMK&station=LFML&station=LFMN&station=LFMO&station=LFMP&station=LFMT&station=LFMU&station=LFMV&station=LFMX&station=LFMY&station=LFNA&station=LFNB&station=LFNM&station=LFOA&station=LFOB&station=LFOC&station=LFOE&station=LFOF&station=LFOH&station=LFOI&station=LFOJ&station=LFOK&station=LFOP&station=LFOR&station=LFOS&station=LFOT&station=LFOV&station=LFOW&station=LFPB&station=LFPC&station=LFPG&station=LFPM&station=LFPN&station=LFPO&station=LFPT&station=LFPV&station=LFPW&station=LFPY&station=LFQA&station=LFQB&station=LFQE&station=LFQG&station=LFQH&station=LFQI&station=LFQQ&station=LFQV&station=LFRA&station=LFRB&station=LFRC&station=LFRD&station=LFRG&station=LFRH&station=LFRI&station=LFRJ&station=LFRK&station=LFRL&station=LFRM&station=LFRN&station=LFRO&station=LFRQ&station=LFRS&station=LFRT&station=LFRU&station=LFRV&station=LFRZ&station=LFSA&station=LFSB&station=LFSC&station=LFSD&station=LFSF&station=LFSG&station=LFSI&station=LFSL&station=LFSN&station=LFSO&station=LFSP&station=LFSQ&station=LFSR&station=LFST&station=LFSX&station=LFTF&station=LFTH&station=LFTU&station=LFTW&station=LFVP&station=LFXA&station=LFXI&station=LFYG&station=LFYH&station=LFYJ&station=LFYL&station=LFYR&data=all&year1=2001&month1=1&day1=1&year2=2010&month2=12&day2=31&tz=Etc%2FUTC&format=onlycomma&latlon=yes&missing=M&trace=T&direct=yes&report_type=1&report_type=2```

# Execution sur serveur 
```ssh thbourge@nf26-1.leger.tf```
mdp ***********
```cd project/```

Si on veut récupérer fichier en sshf :
```sshfs thbourge@nf26-1.leger.tf: ./tmp```
```cd ./tmp/project```
```code .``` (si VS code)


# Structure 

On note X le numéro de l'objectif et XX la version de de la question X. 

* Pour chaque problématique, nous avons un fichier de stockage qui charge les données en fonction de l'objetcif (cf. loadDataQXX.py)
* Par la suite le traitement s'effectue par processingQXX.py
* Une interface main.py se charge des intéractions avec les utilisateurs 
* Un dossier images contient tous les graphes et les cartes html générées

# Lancement 

Exécutez ```python3 main.py``` et suivez les instructions.

# Objectif individuel
Décommentez l'exemple en bas des fichiers processingQXX.py et éxécutez ```python3 processingQXX.py```

