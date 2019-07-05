import stockageBase as stockageBase

class StockageSharding:
    def __init__(self, n):
        self._shards = [stockageBase.StockageBase() for i in range(n)]
        self._n = n

    # def gethashKey(i,n):
    #     return hash(i)%n

    def readAll(self):
        for i in range(self._n):
            print(self._shards[i].readAll())
        return 

    def read(self, k):
        return self._shards[hash(k)%self._n].read(k)

    def create(self, k, v):
        return self._shards[hash(k)%self._n].create(k,v)
        
    def update(self, k, v):
        return self._shards[hash(k)%self._n].update(k,v)

    def delete(self, k):
        return self._shards[hash(k)%self._n].delete(k)
