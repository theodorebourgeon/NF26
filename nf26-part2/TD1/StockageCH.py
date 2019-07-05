import stockageBase as stockageBase
import hashlib

monhash = lambda x: int(hashlib.sha512(x.encode()).hexdigest()[0:16],16)

class StockageCH:
    def __init__(self, n):
        self._stores = [stockageBase.StockageBase() for i in range(n)]
        self._ranges = [(2**64*i//n,2**64*(i+1)//n) for i in range(n)]
        self._n = n

    def getIntervalle(self, k):
        for i in self._ranges:
            if self._ranges[i][0]<k<self._ranges[i][1]:
                return i  

    def readAll(self):
        for i in range(self._n):
            print(self._stores[i].readAll())
        return    

    def read(self, k):
        return self._stores[self.getIntervalle(monhash(k))].read(k)

    def create(self, k, v):
        return self._stores[self.getIntervalle(monhash(k))].create(k,v)          
        
    def update(self, k, v):
        return self._stores[self.getIntervalle(monhash(k))].update(k,v)

    def delete(self, k):
        return self._stores[self.getIntervalle(monhash(k))].delete(k)