#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 8-mapper-ventes.py — TP final, question 1 : le nombre d'articles par ville.
# Lit le CSV des ventes (dataw_fro03) avec csv.reader — le champ libobj peut
# contenir des virgules entre guillemets, un split(',') naïf casserait les
# colonnes. Émet (villecli, qte) au format TABULATION.
#
# Les trois protections vues au chapitre 04, réutilisées telles quelles :
#   1. l'en-tête est sauté par un test de VALEUR (row[0] == 'codcli'), PAS par
#      next() : l'en-tête n'existe que dans le 1er split, un mapper
#      supplémentaire jetterait une ligne de données valide ;
#   2. int(qte) est protégé : une ligne non numérique ne doit pas tuer le job ;
#   3. une ville vide est ignorée (pas de clé vide dans le part-00000).
#
# Colonnes utiles du fichier FRO03 (25 colonnes au total) :
#   5  villecli · 7 datcde (AAAA-MM-JJ) · 15 qte (articles) · 17 libobj
#
# Usage (test local, avant le cluster) :
#   cat DATA/dataw_fro03_mini_1000.csv | python3 8-mapper-ventes.py
#   cat DATA/dataw_fro03_mini_1000.csv | python3 8-mapper-ventes.py | sort -t $'\t' -k1,1 | python3 10-reducer-ventes.py

import csv
import sys

csv_reader = csv.reader(sys.stdin, delimiter=',')

for row in csv_reader:
    # lignes incomplètes ou en-tête : test de valeur, PAS next()
    if len(row) < 25 or row[0] == 'codcli':
        continue

    ville = row[5]          # colonne 5 : villecli
    if not ville:
        continue

    # conversion protégée : une exception ferait échouer le task entier
    try:
        qte = int(row[15])  # colonne 15 : quantité d'articles
    except ValueError:
        continue

    print("%s\t%d" % (ville, qte))