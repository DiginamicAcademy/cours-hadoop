#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 9-mapper-ventes-annee.py — TP final, question 2 : le nombre d'articles par
# couple (ville, année). Même protection que le mapper 8, PLUS une garde sur
# l'année : la colonne 7 (datcde) contient quelques valeurs invalides
# ("NULL" dans le fichier complet), un mapper qui ne les filtre pas enverrait
# une clé "ville#NULL" au reducer.
#
# La clé est COMPOSITE : "ville#2024". C'est le même geste que la clé
# composite danceability+energy du module précédent — chaque composante est
# accolée dans UNE clé textuelle, le reducer voit une valeur unique.
#
# Usage (test local) :
#   cat DATA/dataw_fro03_mini_1000.csv | python3 9-mapper-ventes-annee.py | sort -t $'\t' -k1,1 | python3 10-reducer-ventes.py
#
# Résultat attendu (mini) : LE MANS#2006 en tête avec 30 articles.

import csv
import sys

csv_reader = csv.reader(sys.stdin, delimiter=',')

for row in csv_reader:
    if len(row) < 25 or row[0] == 'codcli':
        continue

    ville = row[5]
    if not ville:
        continue

    try:
        qte = int(row[15])
    except ValueError:
        continue

    # garde sur l'année : 4 chiffres, sinon ligne ignorée
    annee = row[7][:4] if row[7] != 'NULL' else ''
    if not annee.isdigit() or len(annee) != 4:
        continue

    print("%s#%s\t%d" % (ville, annee, qte))