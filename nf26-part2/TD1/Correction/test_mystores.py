import pytest
import mystores
import itertools
import functools

STOCKAGES_A_TESTER = (
    [mystores.StockageBase]
    + [functools.partial(mystores.StockageSharding, n=w) for w in range(1, 10)]
    + [
        functools.partial(
            mystores.StockageSharding,
            n=w1,
            storage=functools.partial(mystores.StockageSharding, n=w2),
        )
        for w1 in range(1, 5)
        for w2 in range(1, 5)
    ]
    + [
        functools.partial(mystores.StockageConsistantHashing, n=n, r=r)
        for n in range(50, 65)
        for r in (3, 5, 7)
    ]
)


@pytest.mark.parametrize("stockage", STOCKAGES_A_TESTER)
def test_stockage(stockage):
    mon_stockage = stockage()
    with pytest.raises(KeyError):
        mon_stockage.read("toto")
    mon_stockage.create("toto", 1)
    assert mon_stockage.read("toto") == 1
    mon_stockage.update("toto", 2)
    assert mon_stockage.read("toto") == 2
    with pytest.raises(KeyError):
        mon_stockage.update("tutu", 1)
    mon_stockage.delete("toto")
    with pytest.raises(KeyError):
        mon_stockage.read("toto")
    with pytest.raises(KeyError):
        mon_stockage.delete("toto")


DATA = [("K{}".format(i), i) for i in range(5)]

REPLICATES_CH = [(r, n) for r in (2, 3, 4) for n in range(25, 50)]


@pytest.mark.parametrize("args", REPLICATES_CH)
def test_replicates_ch(args):
    r, n = args
    for drop_number in range(r):
        for nodes_to_drop in itertools.combinations(range(n), drop_number):
            thestore = mystores.StockageConsistantHashing(
                n=n, r=r, storage=mystores.StockageBase
            )
            for k, v in DATA:
                thestore.create(k, v)
            for i in nodes_to_drop:
                thestore._stores[i].working = False
            for k, v in DATA:
                assert thestore.read(k) == v


@pytest.mark.parametrize("args", REPLICATES_CH)
def test_replicates_ch_fail(args):
    r, n = args
    with pytest.raises(mystores.NotWorking):
        for nodes_to_drop in itertools.combinations(range(n), r):
            thestore = mystores.StockageConsistantHashing(
                n=n, r=r, storage=mystores.StockageBase
            )
            for k, v in DATA:
                thestore.create(k, v)
            for i in nodes_to_drop:
                thestore._stores[i].working = False
            for k, v in DATA:
                v2 = thestore.read(k)
