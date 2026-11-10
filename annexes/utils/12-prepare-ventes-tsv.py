#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 12-prepare-ventes-tsv.py — TP final, question 3 (Hive) : transforme le CSV
# des ventes en TSV que Hive lit sans ambiguïté (FIELDS TERMINATED BY '\t').
#
# Différences volontaires avec le CSV d'origine :
#   - l'en-tête est RETIRÉ (contrairement aux autres jeux du cours) : les
#     requêtes Hive de validation croisée comparent alors des totaux portant
#     exactement sur les 999 lignes (mini) / 135 274 lignes (complet) du
#     reducer MapReduce — sans ligne "datcde" parasite ;
#   - seules 4 colonnes sont gardées, dans l'ordre de la table Hive :
#     villecli · annee (INT) · libobj · qte (INT) ;
#   - une année invalide est écrite \N (NULL SQL) au lieu d'être supprimée,
#     pour ne pas fausser la validation par ville ;
#   - les lignes à qte non numérique sont écartées (comptées) : les MAPPERS
#     les ignorent aussi, MR et Hive restent cohérents.
#
# Usage :
#   python3 12-prepare-ventes-tsv.py DATA/dataw_fro03_mini_1000.csv ventes-clean.tsv
#
# Résultat attendu (manuel auto-vérifié) : 999 lignes de données écrites.

import csv
import sys


def main():
    if len(sys.argv) >= 3:
        fin = open(sys.argv[1], 'r', encoding='utf-8', newline='')
        fout = open(sys.argv[2], 'w', encoding='utf-8', newline='')
    else:
        fin = sys.stdin
        fout = sys.stdout

    reader = csv.reader(fin, delimiter=',', quotechar='"')
    writer = csv.writer(fout, delimiter='\t', lineterminator='\n')

    lues = en_tete = ecartees = annees_nulles = 0

    for row in reader:
        lues += 1
        if not row or len(row) < 25:
            ecartees += 1
            continue
        if row[0] == 'codcli':
            en_tete += 1
            continue                       # en-tête retiré, cf. docstring

        try:
            qte = int(row[15])
        except ValueError:
            ecartees += 1                   # même tolérance que le mapper
            continue

        annee = row[7][:4] if row[7] != 'NULL' else ''
        if not (annee.isdigit() and len(annee) == 4):
            annees_nulles += 1
            annee = '\\N'                   # NULL SQL pour Hive

        writer.writerow([row[5], annee, row[17], qte])

    print("lues        : %d" % lues, file=sys.stderr)
    print("ecartees    : %d (lignes incompletes / qte non numerique)" % ecartees, file=sys.stderr)
    print("en-tete     : %d (retire)" % en_tete, file=sys.stderr)
    print("annees NULL : %d (ecrites \\N)" % annees_nulles, file=sys.stderr)

    if fin is not sys.stdin:
        fin.close()
    if fout is not sys.stdout:
        fout.close()


if __name__ == '__main__':
    main()