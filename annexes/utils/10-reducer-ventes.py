#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 10-reducer-ventes.py — TP final : reducer de structuration (idem
# 2-structure-reducer.py), utilisé par les questions 1 et 2.
# Contrat Hadoop Streaming : le reducer lit sur stdin des paires
# "clé\tvaleur" DÉJÀ TRIÉES par clé. Les valeurs d'une même clé arrivent
# donc CONSÉCUTIVEMENT : on accumule tant que la clé ne change pas, on émet
# au CHANGEMENT de clé et APRÈS la boucle (dans cet ordre — l'oubli de la
# dernière clé est l'erreur classique).
#
# Usage (test local) :
#   cat DATA/dataw_fro03_mini_1000.csv | python3 8-mapper-ventes.py | sort -t $'\t' -k1,1 | python3 10-reducer-ventes.py

import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)     # on sépare clé et valeur sur la 1re TAB
    if len(parts) < 2:
        continue                    # ligne mal formée : on ignore, jamais de crash
    key, value = parts[0], parts[1]

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