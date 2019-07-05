# Implémentation d’une classe de base fournissant CRUD
import stockageBase as stockageBase

print("Stockage classique")
mon_stockage = stockageBase.StockageBase()

# mon_stockage.read('toto') # doit lever une exception
mon_stockage.create('toto',1)
mon_stockage.read('toto') # doit retourner 1
mon_stockage.update('toto',2)
mon_stockage.read('toto') # doit retourner 2
# mon_stockage.update('tutu',1) # doit lever une exception
mon_stockage.delete('toto')
# mon_stockage.read('toto') # doit lever une exception
print(mon_stockage.readAll())

# Implémentation d’une classe fournissant CRUD faisant du sharding sans réplication 
import stockageSharding as stockageSharding

print("Stockage Shard")
mon_shard = stockageSharding.StockageSharding(4)

# mon_shard.read('toto') # doit lever une exception
mon_shard.create('toto',1)
mon_shard.read('toto') # doit retourner 1
mon_shard.update('toto',2)
mon_shard.read('toto') # doit retourner 2
# mon_shard.update('tutu',1) # doit lever une exception
mon_shard.delete('toto')
# mon_shard.read('toto') # doit lever une exception
print(mon_shard.readAll())
 
# Consistant hashing

import StockageCH as StockageCH

print("Stockage Shard")
mon_CH = StockageCH.StockageCH(4)

# mon_shard.read('toto') # doit lever une exception
mon_CH.create('toto',1)
mon_CH.read('toto') # doit retourner 1
mon_CH.update('toto',2)
mon_CH.read('toto') # doit retourner 2
# mon_shard.update('tutu',1) # doit lever une exception
mon_CH.delete('toto')
# mon_shard.read('toto') # doit lever une exception
print(mon_CH.readAll())