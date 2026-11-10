#!/bin/bash
#
# 0-job.sh — lance un job Hadoop Streaming de bout en bout.
#
# Usage :
#   ./0-job.sh [fichier_entrée_local] [nom_sortie_hdfs]
#
#   ./0-job.sh                                    # mots.txt -> output/wordcount
#   ./0-job.sh mots.txt wordcount
#   ./0-job.sh Spotify_Most_Streamed_Songs.csv streams_par_annee
#
# Le script doit être lancé depuis le répertoire qui contient mapper.py,
# reducer.py et le fichier d'entrée. Copiez-le à côté de vos scripts, ou
# adaptez MAPPER / REDUCER ci-dessous.
#
# Variables d'environnement :
#   MAPPER=mapper.py         script mapper local (défaut : mapper.py)
#   REDUCER=reducer.py       script reducer local (défaut : reducer.py)
#   SKIP_START=1             ne pas démarrer HDFS/YARN (services déjà lancés)
#
# Ce job écrit son résultat sur stdout (fichiers part-* dans HDFS). Pour un
# reducer HBase (utils/5-reducer-hbase.py), ce script ne suffit pas : démarrez
# HBase et Thrift à part et supprimez la lecture finale du résultat.

set -euo pipefail

# --- Paramètres -------------------------------------------------------------

INPUT_FILE="${1:-mots.txt}"
OUTPUT_NAME="${2:-wordcount}"
MAPPER="${MAPPER:-mapper.py}"
REDUCER="${REDUCER:-reducer.py}"

INPUT_DIR="input"
OUTPUT_DIR="output/$OUTPUT_NAME"

# --- Vérifications locales : échouer tôt, avec un message clair -------------

if [ -z "${HADOOP_HOME:-}" ]; then
    echo "ERREUR : HADOOP_HOME n'est pas défini (sourcez l'environnement Hadoop)." >&2
    exit 1
fi

STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -n 1 || true)
if [ -z "$STREAMING_JAR" ]; then
    echo "ERREUR : hadoop-streaming-*.jar introuvable sous $HADOOP_HOME/share/hadoop/tools/lib/." >&2
    exit 1
fi

for f in "$INPUT_FILE" "$MAPPER" "$REDUCER"; do
    if [ ! -f "$f" ]; then
        echo "ERREUR : fichier absent : $f (lancez le script depuis son répertoire)." >&2
        exit 1
    fi
done

# --- Diagnostic en cas d'échec ---------------------------------------------

trap 'echo "ÉCHEC ligne $LINENO. Diagnostics : yarn application -list ; puis yarn logs -applicationId <application_id>" >&2' ERR

# --- 1. Services HDFS + YARN ------------------------------------------------

if [ "${SKIP_START:-0}" != "1" ]; then
    # ./start-* et pas start-* : les scripts du cours sont dans /home, pas dans
    # le PATH ; un start-dfs.sh nu résoudrait celui de $HADOOP_HOME/sbin.
    ./start-dfs.sh
    ./start-yarn.sh
fi

# --- 2. Entrée HDFS ---------------------------------------------------------

hdfs dfs -mkdir -p "$INPUT_DIR"
hdfs dfs -put -f "$INPUT_FILE" "$INPUT_DIR"

# --- 3. Sortie : Hadoop refuse un répertoire existant, même vide ------------

hdfs dfs -rm -r -f "$OUTPUT_DIR"
hdfs dfs -mkdir -p output

# --- 4. Job Hadoop Streaming ------------------------------------------------

hadoop jar "$STREAMING_JAR" \
    -file "$MAPPER"  -mapper "python3 $(basename "$MAPPER")" \
    -file "$REDUCER" -reducer "python3 $(basename "$REDUCER")" \
    -input "$INPUT_DIR/$(basename "$INPUT_FILE")" \
    -output "$OUTPUT_DIR"

# --- 5. Contrôle du succès, puis résultat -----------------------------------

if ! hdfs dfs -test -e "$OUTPUT_DIR/_SUCCESS"; then
    echo "ERREUR : pas de _SUCCESS dans $OUTPUT_DIR — le job a échoué." >&2
    echo "Diagnostic : yarn application -list ; yarn logs -applicationId <application_id>" >&2
    exit 1
fi

hdfs dfs -ls "$OUTPUT_DIR"
hdfs dfs -cat "$OUTPUT_DIR"/part-*
