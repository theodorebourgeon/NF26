"""
Various TD1 NF26 storages
"""

import math
import hashlib

monhash = lambda x: int(hashlib.sha512(x.encode()).hexdigest()[0:16], 16)


class NotWorking(Exception):
    pass


class StockageBase:
    """CRUD interface based on dict"""

    def __init__(self):
        self._content = {}
        self.working = True

    def read(self, key):
        """Read a entry from key."""
        if not self.working:
            raise NotWorking
        return self._content[key]

    def create(self, key, val):
        """Create a entry. If key exists, update entry."""
        self._content[key] = val

    def update(self, key, val):
        """Update a existing entry."""
        if key not in self._content:
            raise KeyError(key)
        self._content[key] = val

    def delete(self, key):
        """Delete a existing entry."""
        del self._content[key]


class StockageSharding:
    """CRUD interface based on sharding"""

    def __init__(self, n, storage=StockageBase):
        self._stores = [storage() for i in range(n)]
        self._n = n

    def sharding_fun(self, key):
        """Which node store which key."""
        return hash(key) % self._n

    def read(self, key):
        """Read a entry from key."""
        node = self.sharding_fun(key)
        return self._stores[node].read(key)

    def create(self, key, val):
        """Create a entry. If key exists, update entry."""
        node = self.sharding_fun(key)
        self._stores[node].create(key, val)

    def update(self, key, val):
        """Update a existing entry."""
        node = self.sharding_fun(key)
        self._stores[node].update(key, val)

    def delete(self, key):
        """Delete a existing entry."""
        node = self.sharding_fun(key)
        self._stores[node].delete(key)


class StockageConsistantHashing:
    """CRUD interface based on consistant hasing"""

    def __init__(self, n, r, storage=StockageBase):
        self._stores = [storage() for i in range(n)]
        self._n = n
        self._r = r
        self._ranges = []
        lenrange = math.ceil(2 ** 64 * r / n)
        self._ranges = [
            (i * lenrange % (2 ** 64), (i + 1) * lenrange % (2 ** 64)) for i in range(n)
        ]

    def _get_stores(self, key):
        """Which nodes store which key."""
        key_hash = monhash(key)
        return (
            store
            for (store, (a, b)) in zip(self._stores, self._ranges)
            if (a < b and a <= key_hash < b) or (b < a and not b < key_hash <= a)
        )

    def read(self, key):
        """Read a entry from key."""
        for store in self._get_stores(key):
            try:
                readed = store.read(key)
            except NotWorking:
                continue
            return readed
        raise NotWorking

    def create(self, key, val):
        """Create a entry. If key exists, update entry."""
        for store in self._get_stores(key):
            store.create(key, val)

    def update(self, key, val):
        """Update a existing entry."""
        for store in self._get_stores(key):
            store.update(key, val)

    def delete(self, key):
        """Delete a existing entry."""
        for store in self._get_stores(key):
            store.delete(key)
