#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 3-mapper-txt.py — mapper sur fichier texte : découpe en mots et compte
# chaque mot (le "WordCount"). Sert pour un texte brut (words.txt, un log…),
# PAS pour le CSV Spotify : ses champs contiennent des virgules quotées qu'un
# découpage naïf casserait — utilisez 4-mapper-csv-reader.py pour le CSV.
#
# Usage (test local) :
#   cat words.txt | python3 3-mapper-txt.py
#   cat words.txt | python3 3-mapper-txt.py | sort | python3 2-structure-reducer.py

import sys

for line in sys.stdin:
    line = line.strip()
    for word in line.split():
        if len(word) > 0:
            print("%s\t%s" % (word.lower(), 1))