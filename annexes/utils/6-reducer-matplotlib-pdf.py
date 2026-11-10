#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 6-reducer-matplotlib-pdf.py — reducer qui agrège (clé, total) par année puis
# ENVOIE le résultat sur stdout (pour le part-00000) et GÉNÈRE un graphe PDF.
#
# ⚠️ Trois règles d'exécution, mesurées sur le cluster :
#   1. matplotlib.use('Agg') AVANT d'importer pyplot : sans display, le cluster
#      n'a pas d'écran — sans Agg, l'import plante (IOError).
#   2. Le PDF est écrit dans le RÉPERTOIRE COURANT du task (./resultat.pdf),
#      pas un chemin absolu fantôme comme /datavolume1/... ; puis poussé dans
#      HDFS à la fin.
#   3. Un reducer est un PROCESSUS lancé 1 à N fois : si vous générez le graphe
#      dans la boucle de lecture, vous n'aurez qu'un morceau par task.
#      L'agrégation complète se termine à la FIN du stdin (> et un seul task
#      peut écrire un graphe complet — en pratique : lancez le job avec 1
#      reducer, -D mapreduce.job.reduces=1).
#
# Usage (test local) :
#   cat ma-file.txt | python3 6-reducer-matplotlib-pdf.py          # PDF local
# puis sur le cluster :
#   hdfs dfs -put resultat.pdf /user/root/
#
# Le PDF est redéfini en agrégation PAR ANNÉE pour l'exercice "streams par année".
# Sautez les lignes dont la clé n'est pas numérique (années).

import sys

# 1. Agg AVANT pyplot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# --- agrégation : cumuls par année (="clé") ---
totals = {}          # année -> somme
for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)      # séparateur du contrat : TAB
    if len(parts) < 2:
        continue
    year, value = parts[0], parts[1]
    try:
        value = int(value)
    except ValueError:
        continue
    if year.isdigit():
        totals[year] = totals.get(year, 0) + value

# tri croissant des années pour un graphe lisible
years = sorted(int(y) for y in totals.keys())
sums = [totals[str(y)] for y in years]

# --- sortie standard : les agrégats (part-00000), pour la validation croisée ---
for y in years:
    print("%s\t%s" % (y, totals[str(y)]))

# --- visualisation : le graphe SEULEMENT après la lecture complète du stdin ---
plt.figure(figsize=(10, 8))
plt.bar([str(y) for y in years], sums)
plt.xlabel('Année de sortie')
plt.ylabel('Streams')
plt.title('Streams par année de sortie')
plt.tight_layout()
plt.savefig('resultat.pdf')
print("PDF genere : resultat.pdf (%d annees)" % len(years), file=sys.stderr)