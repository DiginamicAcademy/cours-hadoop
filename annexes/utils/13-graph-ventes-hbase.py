#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 13-graph-ventes-hbase.py — TP final, visualisation : lit la table HBase
# 'ventes' (rowkey = ville, colonne stat:total) et produit le graphique PDF
# des 10 villes qui achètent le plus, plus la liste sur stdout.
#
# Two règles d'exécution (issues du chapitre 04) :
#   1. matplotlib.use('Agg') AVANT d'importer pyplot : le cluster n'a pas
#      d'écran, sans Agg l'import plante (IOError) ;
#   2. un script de visualisation n'est PAS un reducer : il tourne en dehors
#      de MapReduce, juste après le job HBase, sur le MASTER — de là il peut
#      ouvrir le Thrift (port 9090) et lire la table.
#
# Usage (depuis le master, Thrift démarré) :
#   python3 13-graph-ventes-hbase.py
# Résultat attendu (mini) : top 10 avec LE MANS (30 articles) en tête.

import sys

import happybase

# 1. Agg AVANT pyplot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

conn = happybase.Connection('127.0.0.1', 9090)
table = conn.table('ventes')

totaux = {}
for key, cells in table.scan():
    val = cells.get(b'stat:total', b'0')
    try:
        totaux[key.decode()] = int(val)
    except ValueError:
        continue
conn.close()

top = sorted(totaux.items(), key=lambda kv: -kv[1])[:10]

# sortie standard : la liste, pour la vérification / le rapport
for ville, total in top:
    print("%s\t%d" % (ville, total))

# graphique : seulement après la lecture complète de la table
plt.figure(figsize=(10, 8))
plt.bar([v for v, _ in top][::-1], [t for _, t in top][::-1])
plt.xlabel('Ville')
plt.ylabel('Articles vendus')
plt.title('Top 10 des villes par articles vendus')
plt.tight_layout()
plt.savefig('resultat.pdf')
print("PDF genere : resultat.pdf (%d villes)" % len(top), file=sys.stderr)