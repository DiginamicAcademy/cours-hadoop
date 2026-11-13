#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 9-mapper-streams-annee.py — TP final, question 2 : les streams cumulés par
# couple (artiste, année). Même protection que le mapper 8, PLUS une garde sur
# l'année : la colonne 3 (released_year) doit être une année à 4 chiffres, un
# mapper qui ne filtre pas enverrait une clé "artiste#" ou "artiste#NULL" au
# reducer.
#
# La clé est COMPOSITE : "artiste#2022". C'est le même geste que la clé
# composite du module précédent — chaque composante est accolée dans UNE clé
# textuelle, le reducer voit une valeur unique.
#
# Usage (test local) :
#   cat DATA/Spotify_Most_Streamed_Songs.csv | python3 9-mapper-streams-annee.py \
#     | sort -t $'\t' -k1,1 | python3 10-reducer-somme.py
#
# Résultat attendu : Bad Bunny#2022 en tête (7 436 274 050 streams).

import csv
import sys

csv_reader = csv.reader(sys.stdin, delimiter=',')

for row in csv_reader:
    if len(row) < 9 or row[0] == 'track_name':
        continue

    artiste = row[1]
    if not artiste:
        continue

    try:
        streams = int(float(row[8]))
    except ValueError:
        continue

    # garde sur l'année : 4 chiffres, sinon ligne ignorée
    annee = row[3]
    if not annee.isdigit() or len(annee) != 4:
        continue

    print("%s#%s\t%d" % (artiste, annee, streams))
