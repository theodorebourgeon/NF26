pip3 install cassandra-driver 

ssh thbourge@nf26-1.leger.tf
mdp **** voir mail --> tous les users en lecture sur le serveur (aller voir binome)

doc : 

* http://datastax.github.io/python-driver/getting_started.html 
* http://cassandra.apache.org/doc/latest/getting_started/querying.html 

cqlsh :

* DESCRIBE KEYSPACES : afficher keyspace
* use <keyspace>;
* DESCRIBE TABLES : afficher tables 
* get colonnes table : SELECT * FROM system_schema.columns 
WHERE keyspace_name = 'cycling' AND table_name = 'cyclist_name';
* DROP TABLE [IF EXISTS] keyspace_name.table_name

sshfs p
Permet de linker repertoire courant avec serveur
sshfs thbourge@nf26-1.leger.tf: ~/Desktop Documents/UTC/GI05/NF26/nf26-g/TD3

[Options]
-o nonempty : allow mounts over non-empty file/dir

Démonter : fusermount -u <dir>  

If Error : 
Find the culprit sshfs process:
$ pgrep -lf sshfs
Kill it:
$ kill -9 <pid_of_sshfs_process>
sudo force unmount the "unavailable" directory:
$ sudo umount -f <mounted_dir>


# SPARK 

NF26 
Spark 
> source /pyspark.env
Pyspark 
Exit 
Cd /exdata (ou les données sont) 
Pypark 

Import générateur chargement de donnée
# créer sc —> spart contexte dans lequel on peut mettre des données et les manipuler 
D1 = sc.parallelize(citibike.moadata(«  djhfkjsadbhf.csv»))

#D1 est un jeu de donnée (se comporte comme une liste d’élément) 
D1.take(10) —> recupère 10 données
#On ne fait pas des map/red sur toutes les données de base, on en prends que des paquets par paquets

# MAPPING
Dmap = D1.map(lambda d: (d[‘starttime’][3], (1, d[‘tripduration])

Map.take(2)

#REDUCTION
Def st2(x,y):
	return x[0]+y[0],x[1]+y[1]
Dred = Map.reducedByKey(st2)
Dred.take(2)
Dred.collect —> sur toutes les données


Def limgen(gen,limit):
	for i,d in enumerate(gen):
		if i>=limit:
			return None
		yield d


#quantile 

import scipy as sp

def mamap(d):
    h = d['starttime'][3]
    t = d['tripduration']
    return (h, (t,t))

def mared(x,y):
    xm,xM = x
    ym, yM = Y
    return min(xm,ym), max(xM,yM)

D1 = .....

D1.collect()

connaissance ={ h: ([0,1],[m,M]) for h, (m,M) in _39}

connaissance

proposed_quartiles = {h: sp.iterp([.25,.5,.75],p,x) for (h, (p,x)) in connaissance.items()}

proposed_quartiles

def compare(x, quartiles):
    return (1, x<quartiles[0], x<quartiles[1], x<quartiles[2])

D.map(lambda d: d['starttime'][3], compare(d['tripduration'], proposed_qiartiles[d['satrttime'][3]])).reduceByKey(lambda a,b: tuple (x+y in zip(a,b))

puis 
.map(lambda w: (w[0], (w[1][1]/w[1][0], w[1][2]/w[1][0], w[1][3]/w[1][0])))

puis .collect()

for h, fs in R:
    connaissance[0].extend(fs)
    connaissance[1].extend(proposed_quartiles[h])
    connaissance[h][0].sort
    connaissance[h][1].sort

on a effetcué une seule itération 


Il faut boucler jusqu`à convergence :

for i in range(5):
    proposed_quartiles = {h: sp.iterp([.25,.5,.75],p,x) for (h, (p,x)) in connaissance.items()}

    R = D.map......collect
    for h, fs in R:
        connaissance[0].extend(fs)
        connaissance[1].extend(proposed_quartiles[h])
        connaissance[h][0].sort
        connaissance[h][1].sort

Normalement on calcul tous les centiles en meme temps, comme ca on a tout 