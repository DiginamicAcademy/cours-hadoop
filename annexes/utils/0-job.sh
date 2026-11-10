#!/bin/bash

# 0-job.sh — script d'automatisation d'un job MapReduce "type" du cours.
# Le job type écrit sur stdout (pas HBase). Ajustez INPUT/OUTPUT et les noms
# de mapper/reducer ; pour les reducers happybase, voyez utils/5 (Thrift est
# lance à part : ./start-hbase.sh && ./start-thrift.sh).

INPUT_FILE="Spotify_Most_Streamed_Songs.csv"
OUTPUT_FILE="exemple"

# Démarrage HDFS + YARN (l'ordre des dépendances est géré par start-all.sh).
# NB : ./start-* et pas start-* : /home n'est pas dans le PATH, un start-dfs.sh
# nu résoudrait le script d'origine d'Hadoop ($HADOOP_HOME/sbin) — pas le nôtre.
./start-dfs.sh                    # NameNode & DataNodes (la 1re fois : format)
./start-yarn.sh                   # ResourceManager & NodeManagers
# (pas de HBase/Thrift ici : ce job n'en a pas besoin)

# Répertoire d'entrée : le CSV Spotify hébergé par le TP 03
hdfs dfs -mkdir -p input
hdfs dfs -put -f $INPUT_FILE input

# Le répertoire de sortie ne doit pas exister : on le vide avant chaque run
hdfs dfs -rm -r -f output/$OUTPUT_FILE
hdfs dfs -mkdir -p output

# Lancement du job Hadoop avec les scripts mapper et reducer
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-file mapper.py -mapper "python3 mapper.py" \
-file reducer.py -reducer "python3 reducer.py" \
-input input/$INPUT_FILE \
-output output/$OUTPUT_FILE

# Afficher le résultat
hdfs dfs -cat output/$OUTPUT_FILE/part-00000