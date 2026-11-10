#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# prepare-spotify-tsv : lit le CSV Spotify (champs quotes pouvant contenir
# des virgules) et produit un TSV (champs separes par des tabulations) que
# Hive lit sans ambiguete avec FIELDS TERMINATED BY '\t'.
# Compatible Python 3.5+ (le cluster tourne en 3.11).
#
# Usage :
#   python3 7-prepare-spotify-tsv.py Spotify_Most_Streamed_Songs.csv spotify-clean.tsv
# ou en flux (stdin -> stdout) :
#   cat Spotify_Most_Streamed_Songs.csv | python3 7-prepare-spotify-tsv.py > spotify-clean.tsv
#
# Le script COMPTE ce qu'il fait et l'affiche sur stderr (lues / ecrites /
# ecartees, et surtout le nombre de champs "streams" non numeriques :
# c'est ce compteur, pas le nombre de colonnes, qui revele la ligne 576
# corrompue du jeu fourni — 25 colonnes ne garantissent pas 25 champs sains).

import csv
import sys

try:
    from io import StringIO  # Python 3
except ImportError:  # pragma: no cover
    from StringIO import StringIO  # Python 2 (au cas ou)


def main():
    if len(sys.argv) >= 3:
        fin = open(sys.argv[1], 'r', encoding='utf-8', newline='')
        fout = open(sys.argv[2], 'w', encoding='utf-8', newline='')
    else:
        fin = sys.stdin
        fout = sys.stdout

    reader = csv.reader(fin, delimiter=',', quotechar='"')
    writer = csv.writer(fout, delimiter='\t', lineterminator='\n')

    lues = ecartees = 0
    streams_non_numeriques = 0

    for row in reader:
        lues += 1
        # lignes vides / mal formees
        if not row or len(row) != 25:
            ecartees += 1
            continue

        # en-tete : il a bien 25 colonnes, on le garde tel quel (la table Hive
        # n'utilise pas skip.header.line.count). On ne lui compte pas de defaut.
        if row[0] == 'track_name':
            writer.writerow(row)
            continue

        # champs "streams" non numeriques : la ligne 576 corrompue est ecrite
        # (c'est le piege enseignant du cours), mais comptee.
        try:
            int(float(row[8]))
        except ValueError:
            streams_non_numeriques += 1

        writer.writerow(row)

    print("lues      : %d" % lues, file=sys.stderr)
    print("ecrites   : %d" % (lues - ecartees), file=sys.stderr)
    print("ecartees  : %d" % ecartees, file=sys.stderr)
    print("streams non numeriques : %d (ligne 576 du jeu fourni)" % streams_non_numeriques,
          file=sys.stderr)

    if fin is not sys.stdin:
        fin.close()
    if fout is not sys.stdout:
        fout.close()


if __name__ == '__main__':
    main()