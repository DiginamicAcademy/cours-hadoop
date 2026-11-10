#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 2-structure-reducer.py — squelette minimal d'un reducer.
# Contrat Hadoop Streaming : le reducer lit sur stdin des paires
# "clé\tvaleur" DÉJÀ TRIÉES par clé (c'est le shuffle qui trie). Les valeurs
# d'une même clé arrivent donc de manière CONSECUTIVE : on accumule tant que
# la clé ne change pas, on émet au CHANGEMENT de clé et après la boucle.
#
# Usage (test local, avant le cluster) :
#   echo "bonjour bonsoir bonjour" | python3 1-structure-mapper.py | sort | python3 2-structure-reducer.py

import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)     # on sépare clé et valeur sur la 1re TAB
    if len(parts) < 2:
        continue                    # ligne mal formée : on ignore, jamais de crash
    key, value = parts[0], parts[1]

    # conversion protégée : sur le cluster, une exception tue le task entier
    try:
        value = int(value)
    except ValueError:
        continue

    if current_key == key:          # même clé consécutive : on accumule
        current_total += value
    else:                           # nouvelle clé : on émet la précédente
        if current_key:
            print("%s\t%s" % (current_key, current_total))
        current_key = key
        current_total = value

if current_key:                     # NE JAMAIS OUBLIER la dernière clé
    print("%s\t%s" % (current_key, current_total))