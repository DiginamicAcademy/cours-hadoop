#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 4-mapper-csv-reader.py — mapper lisant un CSV avec csv.reader (respect des
# guillemets : "Latto, Jung Kook" reste UN champ). Émet (clé, valeur) au format
# TABULATION, les deux actions qui empêchent les "bugs silencieux" du TP 04 :
#   1. l'en-tête est sauté par un test de VALEUR (row[0] == 'track_name'),
#      PAS par next() : l'en-tête n'existe que dans le 1er split, un mapper
#      supplémentaire jetterait une ligne de données valide ;
#   2. int() est protégé : la ligne 576 du fichier a un champ streams corrompu
#      (contenu de 9 colonnes fondu en un), sans protection le job ÉCHOUE.
#
# Usage (test local) :
#   cat Spotify_Most_Streamed_Songs.csv | python3 4-mapper-csv-reader.py
#   cat Spotify_Most_Streamed_Songs.csv | python3 4-mapper-csv-reader.py | sort | python3 2-structure-reducer.py

import sys
import csv

# Le reader gère les guillemets CSV ; pas de split(',') naïf.
csv_reader = csv.reader(sys.stdin, delimiter=',')

for row in csv_reader:
    # ignorer les lignes incomplètes ou l'en-tête (test de valeur, PAS next())
    if len(row) < 9 or row[0] == 'track_name':
        continue

    released_year = row[3]   # colonne 3 : année de sortie
    streams = row[8]         # colonne 8 : nombre de streams

    # conversion protégée : une exception ferait échouer le task entier
    try:
        streams = int(float(streams))
    except ValueError:
        continue

    if released_year.isdigit():
        print("%s\t%s" % (released_year, streams))