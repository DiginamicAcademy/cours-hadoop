#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 1-structure-mapper.py — squelette minimal d'un mapper.
# Contrat Hadoop Streaming : le mapper lit des lignes sur stdin, écrit sur
# stdout UNE PAIRE clé-valeur par ligne, séparée par UNE TABULATION.
#   clé<TAB>valeur\n
#
# Usage (test local, avant le cluster) :
#   echo "bonjour bonsoir bonjour" | python3 1-structure-mapper.py
#   cat fichier.txt | python3 1-structure-mapper.py | sort | python3 reducer.py

import sys

# lire l'entrée standard ligne par ligne
for line in sys.stdin:
    line = line.strip()          # retirer l'espace/fin de ligne de début et fin
    if not line:
        continue

    # EXEMPLE : découper en mots et émettre une paire (mot, 1) par mot.
    # C'est LA forme du contrat : "clé\tvaleur". pas de point-virgule, pas de
    # valeur sans clé : le shuffle regroupe PAR LA CLÉ.
    for word in line.split():
        print("%s\t%s" % (word, 1))

    # Votre traitement à vous remplace ce qui précède : extraire vos champs,
    # convertir si besoin, puis émettre (clé, valeur).