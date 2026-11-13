#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 8-mapper-streams-artiste.py — TP final, question 1 : les streams cumulés par
# artiste. Lit le CSV Spotify avec csv.reader — le champ artiste peut contenir
# des virgules entre guillemets ("Latto, Jung Kook" reste UN champ), un
# split(',') naïf casserait les colonnes. Émet (artiste, streams) au format
# TABULATION.
#
# Les trois protections vues au chapitre 04, réutilisées telles quelles :
#   1. l'en-tête est sauté par un test de VALEUR (row[0] == 'track_name'), PAS
#      par next() : l'en-tête n'existe que dans le 1er split, un mapper
#      supplémentaire jetterait une ligne de données valide ;
#   2. int(streams) est protégé : la ligne 576 du fichier a un champ streams
#      corrompu (les valeurs des colonnes numériques d'après, bpm à speechiness,
#      collées bout à bout) — sans protection le job ÉCHOUE ;
#   3. un artiste vide est ignoré (pas de clé vide dans le part-00000).
#
# Colonnes utiles du fichier Spotify (25 colonnes au total) :
#   0 track_name · 1 artist(s)_name · 3 released_year · 8 streams
#
# Usage (test local, avant le cluster) :
#   cat data/Spotify_Most_Streamed_Songs.csv | python3 8-mapper-streams-artiste.py
#   cat data/Spotify_Most_Streamed_Songs.csv | python3 8-mapper-streams-artiste.py \
#     | sort -t $'\t' -k1,1 | python3 10-reducer-somme.py

import csv
import sys

csv_reader = csv.reader(sys.stdin, delimiter=',')

for row in csv_reader:
    # lignes incomplètes ou en-tête : test de valeur, PAS next()
    if len(row) < 9 or row[0] == 'track_name':
        continue

    artiste = row[1]           # colonne 1 : artist(s)_name
    if not artiste:
        continue

    # conversion protégée : une exception ferait échouer le task entier
    try:
        streams = int(float(row[8]))   # colonne 8 : nombre de streams
    except ValueError:
        continue

    print("%s\t%d" % (artiste, streams))
