import processingQ1
import processingQ12
import processingQ2
import processingQ3_kmeans

print("=====================================================")
print(" ")
print("Bienvenue dans l'interface cli du projet en haute volumétrie sur les données méteorologiques Française de 2001 à 2010 !")
print("Objectif 1 : Pour un point donné de l'espace,  je veux pouvoir avoir un historique du passé,  avec des courbes adaptés. Je vous pouvoir mettre en évidence la saisonnalité et les écarts à la saisonnalité.")
print("Objectif 2 : À un instant donné je veux pouvoir obtenir une carte me représentant n'importe quel indicateur.")
print("Objectif 3 : Pour une période de temps donnée,  je veux pouvoir obtenir clusteriser l'espace,  et représenter cette clusterisation.")
print(" ")
print("=====================================================")
print(" ")
print("A quelle question voulez vous répondre ? ")
question = int(input("Rentrez le numéro de l'objectif (1-2-3) ? "))
if (question == 1):
    version = int(input("Considérez-vous un point comme une station (0) ou des coordonnées (1) ? (0/1) "))
    if version == 0:
        station = str(input("Rentrez le nom de la station qui vous intéresse (ex: LFRT) ? "))
        if station: 
            yearOK = str(input("Voulez-vous vous restreindre à une année particulière (y/n) ? "))
            if yearOK == ("y" or "yes" or "Y" or "YES"):
                year = int(input("Quelle année vous interesse (entre 2001 et 2010) ? "))
                if (year <= 2010 and year>=2001):
                    print(" ")
                    processingQ1.question1(station, year)
                else:
                    print("Rentrez une année valide")
            else: 
                print(" ")
                processingQ1.question1(station)
            print(" ")
            print("Vous trouverez les graphiques généré dans le dossier ./images !")
        else: 
            print("Rentrez un nom de station valide")
    elif version == 1:
        lon = float(input("Rentrez la longitude ? (ex. 4.786645) "))
        lat = float(input("Rentrez la latitude ? (ex. 48.267885) "))
        vv = str(input("Voulez vous interroger une unique pavé ou une fenêtre de 9 pavés ? (single/multi) "))
        if vv == "single" or "multi":
            yearOK = str(input("Voulez-vous vous restreindre à une année particulière (y/n) ? "))
            if yearOK == ("y" or "yes" or "Y" or "YES"):
                year = int(input("Quelle année vous interesse (entre 2001 et 2010) ? "))
                if year <= 2010 and year>=2001:
                    print(" ")
                    print(lon,lat,year,vv)
                    processingQ12.question12(lon,lat, year, version=vv)
                else:
                    print("Rentrez une année valide")
            else: 
                print(" ")
                processingQ12.question12(lon,lat, version=vv)
            print(" ")
            print("Vous trouverez les graphiques généré dans le dossier ./images !")       
        else:
            print("Rentrez un choix valide")
    else:
        print("Rentrez une version valide")
elif (question == 2):
    annee = str(input("Quelle année vous interresse (entre 2001 et 2010)? "))
    mois = str(input("Quel mois (entre 1 et 12 )? "))
    jour = str(input("Quel jour (entre 1 et 31)?"))
    heure = str(input("Quelle heure (entre 0 et 24)?"))
    minute = str(input("Quelle periode (00 ou 30) ?"))
    date = f"""{annee}-{mois}-{jour} {heure}:{minute}"""
    print(date)
    if date: 
        print(" ")
        processingQ2.question2(date)
        print(" ")
        print("Vous trouverez les graphiques généré dans le dossier ./images !")
    else: 
        print("Rentrez des champs valide")

elif (question == 3):
    date1 = str(input("Quelle date de début de période ? (format YYYY-MM-DD) ex. 2001-01-01 "))
    date2 = str(input("Quelle date de fin de période ? (format YYYY-MM-DD) ex. 2001-10-31 "))
    k = int(input("Combien de cluster voulez vous faire ? ex. 5"))
    print(" ")
    processingQ3_kmeans.question3(date1, date2, k)
    print(" ")
    print("Vous trouverez les graphiques généré dans le dossier ./images !")
else: 
    print("Veuillez rentrer un numéro entre 1 et 3")
