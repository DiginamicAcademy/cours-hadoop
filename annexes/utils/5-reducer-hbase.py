#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 5-reducer-hbase.py — reducer qui agrège (clé, streams) puis INSÈRE dans HBase.
# ATTENTION à l'ordre : la table est créée UNE SEULE FOIS, AVANT le job.
# Un reducer qui drop/recreate sa table est faux par construction : avec
# 2 reducers, le second détruit les lignes du premier.
#
# Étape 1 — créer la table (une fois, depuis le master, AVANT de soumettre) :
#   python3 - <<'PY'
#   import happybase
#   conn = happybase.Connection('127.0.0.1', 9090)
#   for t in ('streams_year', 'streams_artist'):
#       try:
#           conn.disable_table(t); conn.delete_table(t)
#       except Exception:
#           pass
#   conn.create_table('streams_year',  {'stat': dict()})
#   conn.create_table('streams_artist', {'stat': dict()})
#   conn.close()
#   PY
#
# Étape 2 — lancer le job (dans le master, après ./start-hbase.sh et
# ./start-thrift.sh) :
#   hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
#   -file mapper.py -mapper "python3 mapper.py" \
#   -file 5-reducer-hbase.py -reducer "python3 5-reducer-hbase.py" \
#   -input input/Spotify_Most_Streamed_Songs.csv \
#   -output output/streams_year
#
# Étape 3 — vérifier (le put ne confirme rien) :
#   hbase shell
#   scan 'streams_year'
#
# La clé d'entrée (stdin) : "clé\tagrégat" où clé = année (ex. 1975) pour
# streams_year, ou artiste pour streams_artist. La ROWKEY est cette clé.

import sys
import happybase

IP = '127.0.0.1'                    # Thrift du cluster ; depuis le master
PORT = 9090
TABLE = 'streams_year'              # 'streams_year' ou 'streams_artist'

# agrégation : valeurs consécutives d'une même clé (les mêmes, mais passage
# par l'écriture HBase au lieu du stdout)
current_key = None
current_total = 0

rows = []   # on accumule en mémoire ; flush avant la connexion
for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)         # séparateur du contrat : TAB, pas ';'
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
connection = happybase.Connection(IP, PORT)   # autoconnect=True par défaut
table = connection.table(TABLE)
with table.batch(batch_size=100) as batch:
    for key, total in rows:
        batch.put(
            str(key).encode(),
            {b'stat:total_streams': str(total).encode()}
        )
connection.close()

# sys.stderr : ce qui est écrit sur stdout par un reducer part dans le
# part-00000 ; toute sortie parasite corrompt le résultat. Passez par stderr.
print("reducer 5 : %d lignes inserees dans %s" % (len(rows), TABLE), file=sys.stderr)