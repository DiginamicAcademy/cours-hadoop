#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 12-prepare-streams-tsv.py — TP final, question 3 (Hive) : transforme le CSV
# Spotify en TSV que Hive lit sans ambiguïté (FIELDS TERMINATED BY '\t').
#
# Différences volontaires avec le CSV d'origine :
#   - l'en-tête est RETIRÉ (contrairement au 7-prepare-spotify-tsv du flux
#     NiFi) : les requêtes Hive de validation croisée comparent alors des
#     totaux portant exactement sur les 952 lignes saines du reducer
#     MapReduce — sans ligne "track_name" parasite ;
#   - seules 4 colonnes sont gardées, dans l'ordre de la table Hive :
#     artiste · annee (INT) · titre · streams (BIGINT) ;
#   - une année invalide est écrite \N (NULL SQL) au lieu d'être supprimée,
#     pour ne pas fausser la validation par artiste ;
#   - les lignes à streams non numérique sont écartées (comptées) : les
#     MAPPERS les ignorent aussi (ligne 576 corrompue), MR et Hive restent
#     cohérents.
#
# Usage :
#   python3 12-prepare-streams-tsv.py DATA/Spotify_Most_Streamed_Songs.csv streams-clean.tsv
#
# Résultat attendu : 952 lignes de données écrites, 1 écartée (la 576).

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
        if not row or len(row) < 9:
            ecartees += 1
            continue
        if row[0] == 'track_name':
            en_tete += 1
            continue                       # en-tête retiré, cf. docstring

        try:
            streams = int(float(row[8]))
        except ValueError:
            ecartees += 1                   # même tolérance que le mapper
            continue

        annee = row[3]
        if not (annee.isdigit() and len(annee) == 4):
            annees_nulles += 1
            annee = '\\N'                   # NULL SQL pour Hive

        writer.writerow([row[1], annee, row[0], streams])

    print("lues         : %d" % lues, file=sys.stderr)
    print("ecrites      : %d" % (lues - en_tete - ecartees), file=sys.stderr)
    print("ecartees     : %d (incompletes / streams non numerique — ligne 576)" % ecartees, file=sys.stderr)
    print("en-tete      : %d (retire)" % en_tete, file=sys.stderr)
    print("annees NULL  : %d (ecrites \\N)" % annees_nulles, file=sys.stderr)

    if fin is not sys.stdin:
        fin.close()
    if fout is not sys.stdout:
        fout.close()


if __name__ == '__main__':
    main()
