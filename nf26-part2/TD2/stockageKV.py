class StockageBase:
    def __init__(self):
        self._content = {}
    
    def readAll(self):
        try: 
            return self._content
        except KeyError:
            raise KeyError('EXCEPTION READ')

    def read(self, k):
        try: 
            return self._content[k]
        except KeyError:
            raise KeyError('EXCEPTION READ')

    def create(self, k, v):
        try: 
            self._content.update({k:v})
        except KeyError:
            raise KeyError('EXCEPTION CREATE')
        
    def update(self, k, v):
        if k not in self._content:
            raise KeyError('EXCEPTION UPDATE')
        self._content.update({k:v})

    def delete(self, k):
        if k not in self._content:
            raise KeyError('EXCEPTION DELETE')
        del self._content[k]
