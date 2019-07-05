"""
Module d'import
"""

import csv
import glob


def lire_fichier_liste(fname):
    """
    Lecture données listes
    """
    dial = csv.excel
    dial.delimiter = ";"
    Lret = []
    with open(fname) as filed:
        reader = csv.DictReader(filed, dialect=dial)
        for row in reader:
            Lret.append(dict(row))
    return Lret


def lire_fichier_gen(fname):
    """
    Lecture données générateur
    """
    dial = csv.excel
    dial.delimiter = ";"
    with open(fname) as filed:
        reader = csv.DictReader(filed, dialect=dial)
        for row in reader:
            yield dict(row)


def get_nombre_mutation(fname):
    """
    Récupérer le nombre de mutation d'une section cadastralle
    """
    tot = 0
    for l in lire_fichier_gen(fname):
        tot += 1
    return tot

def get_nombre_mutation_all():
    for i in glob.glob("data/*.csv"):
        print('Pour la parcelle cadastralle', i[-6:-4])
        print('Le nombre de mutation est :', get_nombre_mutation(i))
        print('\n')

def get_best_section():
    return True