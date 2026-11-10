#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 11-reducer-ventes-hbase.py — TP final, question 4 : le reducer écrit le
# total d'articles PAR VILLE dans HBase au lieu de l'envoyer sur stdout.
# ROWKEY = ville, famille de colonnes stat, colonne stat:total.
#
# ⚠️ Règles (vues aux exercices 6-7 du chapitre 04) :
#   1. la table est créée UNE SEULE FOIS, AVANT le job — jamais dans le
#      reducer (avec 2 reducers, le second détruirait les lignes du premier) ;
#   2. un reducer HBase n'écrit RIEN sur stdout (un print corromprait le
#      part-00000) — les messages de suivi partent sur stderr ;
#   3. la vérification est un get/scan : le put ne confirme rien.
#
# Préparer la table (depuis le master, AVANT le job) :
#   ./start-hbase.sh && ./start-thrift.sh
#   python3 - <<'PY'
#   import happybase
#   conn = happybase.Connection('127.0.0.1', 9090)
#   try:
#       conn.disable_table('ventes'); conn.delete_table('ventes')
#   except Exception:
#       pass
#   conn.create_table('ventes', {'stat': dict()})
#   conn.close()
#   PY
#
# Lancer le job (avec le mapper 8, celui de la question 1) :
#   hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
#   -file 8-mapper-ventes.py -mapper "python3 8-mapper-ventes.py" \
#   -file 11-reducer-ventes-hbase.py -reducer "python3 11-reducer-ventes-hbase.py" \
#   -input input/dataw_fro03_mini_1000.csv -output output/ventes_hbase
#
# Vérifier :
#   hbase shell
#   count 'ventes'            # 569 villes sur le mini
#   get 'ventes','LE MANS'    # stat:total = 30

import sys
import happybase

IP = '127.0.0.1'                 # Thrift du cluster ; depuis le master
PORT = 9090
TABLE = 'ventes'

# agrégation : valeurs consécutives d'une même ville
current_key = None
current_total = 0
rows = []

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)          # contrat : TAB
    if len(parts) < 2:
        continue
    key, value = parts[0], parts[1]
    try:
        value = int(value)
    except ValueError:
        continue
    if current_key == key:
        current_total += value
    else:
        if current_key:
            rows.append((current_key, current_total))
        current_key = key
        current_total = value

if current_key:
    rows.append((current_key, current_total))

# connexion puis insertions groupées (une seule fois, à la fin de l'exécution)
connection = happybase.Connection(IP, PORT)
table = connection.table(TABLE)
with table.batch(batch_size=100) as batch:
    for city, total in rows:
        batch.put(city.encode(), {b'stat:total': str(total).encode()})
connection.close()

print("%d villes inserees dans %s" % (len(rows), TABLE), file=sys.stderr)