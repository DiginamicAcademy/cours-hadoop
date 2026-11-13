![Diginamic Logo](../assets/diginamic.png) ![Hadoop Logo](../assets/hadoop.png)

# TP final : Analyse des streams par artiste — toute la pipeline

> **Type** : TP projet de synthèse (évalué) — le seuil TP du cursus
> **Prérequis** : séquences 02 à 07 (environnement, HDFS, MapReduce, Hive, HBase, NiFi)
> **Durée estimée** : une journée (4 h à 5 h) — HDFS → MapReduce → Hive → HBase → NiFi → rendu
> **Données** : `../data/Spotify_Most_Streamed_Songs.csv` — le CSV du fil rouge, celui des EX 03 à 07 (953 chansons). Le TP change de **regard**, pas de donnée.
> **Contexte** : les EX 02 à 07 vous ont fait *appliquer chaque outil séparément* ; ce TP est l'exercice *intégrateur* et évalué — la même donnée traverse toute la chaîne, avec rapport et livrables.

C'est le **TP de synthèse** du cours : vous enchaînez toute la pipeline avec **une seule donnée**, puis vous rédigez la **documentation de vos actions et de leurs retours**. Chaque étape est une réponse à une question — gardez la trace de **ce que vous avez lancé** (la commande), **ce que vous avez observé** (le retour), et **ce que vous en concluez**.

_📌 Il n'est pas nécessaire de mettre tout le code dans le rapport, mais de bien expliquer étape par étape : le lecteur doit pouvoir reproduire vos actions sans votre aide._
_Exemple : utilisation d'une structure minimale._

> 🤔 **Question de contrôle** : notez en cours de route **où** chaque outil intervient dans la chaîne (qui ingère ? qui déplace ? qui stocke ? qui calcule ? qui sert ?). C'est exactement le plan du rapport demandé en livrable.

---

## Sommaire

- [I. Objectif — les questions du TP](#i-objectif--les-questions-du-tp)
- [II. HDFS — accueillir le fichier](#ii-hdfs--accueillir-le-fichier)
- [III. Question 1 — le classement des artistes (MapReduce)](#iii-question-1--le-classement-des-artistes-mapreduce)
- [IV. Question 2 — la clé composite artiste#année (MapReduce)](#iv-question-2--la-clé-composite-artisteannée-mapreduce)
- [V. Question 3 — le même résultat en SQL (Hive)](#v-question-3--le-même-résultat-en-sql-hive)
- [VI. Question 4 — stocker le résultat en clé/valeur (HBase)](#vi-question-4--stocker-le-résultat-en-clévaleur-hbase)
- [VII. Question 5 — ingérer au lieu d'un `put` (NiFi)](#vii-question-5--ingérer-au-lieu-dun-put-nifi)
- [VIII. Visualisation — le top 10 des artistes](#viii-visualisation--le-top-10-des-artistes)
- [Bilan — la chaîne complète](#bilan--la-chaîne-complète)
- [Bonus — le changement de dimension](#bonus--le-changement-de-dimension)
- [Livrables attendus](#livrables-attendus)

---

## I. Objectif — les questions du TP

Le fichier `Spotify_Most_Streamed_Songs.csv` enregistre les **chansons les plus streamées** de la plateforme (titre, artiste(s), année de sortie, audience sur chaque service…). Quatre colonnes vous intéressent (index de la colonne dans la ligne CSV, en partant de 0) :

| Colonne (index) | Nom | Usage |
|---|---|---|
| 0 | `track_name` | le titre de la chanson (la dimension du bonus) |
| 1 | `artist(s)_name` | l'artiste — une collaboration est une chaîne complète : « Latto, Jung Kook » est **un** artiste |
| 3 | `released_year` | l'année de sortie (4 chiffres) |
| 8 | `streams` | le nombre de streams cumulés |

> ⚠️ **Attention** : ce jeu contient **une ligne corrompue** (la 576 du fichier) dont le champ `streams` contient dix valeurs numériques collées bout à bout (de bpm à speechiness). Vos mappers doivent survivre — c'est la leçon du chapitre 04 (conversion protégée) mise à l'épreuve : sans garde, le job **échoue**.

Vous répondez à **plusieurs questions**, dans l'ordre de la chaîne :

| # | Question | Outil | Chapitre d'appui |
|---|---|---|---|
| Q1 | Quels **artistes** cumulent le plus de streams ? | MapReduce | [exercice 04](../exercices/04-mapreduce.md) (exercices 1-3) + [TD 05](../td/TD05.md) |
| Q2 | Même question, mais **par artiste et par année** (clé composite) | MapReduce | [exercice 04](../exercices/04-mapreduce.md) (clé composite) |
| Q3 | Le **même classement en SQL** : les deux moteurs sont-ils d'accord ? | Hive | [exercice 05](../exercices/05-hive.md) + [TD 02](../td/TD02.md) |
| Q4 | Comment retrouver ces totaux **en clé/valeur** ? | HBase | [exercice 06](../exercices/06-hbase.md) + [TD 03](../td/TD03.md) |
| Q5 | Et si le fichier arrivait **ingéré par un flux** plutôt qu'un `put` ? | NiFi | [exercice 07](../exercices/07-nifi.md) + [TD 04](../td/TD04.md) |
| V | Le **graphique** du top 10 des artistes | Matplotlib | [exercice 06](../exercices/06-hbase.md) (partie IV) |

Vous utiliserez **Hadoop MapReduce** pour calculer, **HappyBase** pour stocker, **Hive** pour valider en SQL, **NiFi** pour ingérer, et **Matplotlib** pour visualiser.

> **Aides** : scripts de référence dans [`../annexes/utils/`](../annexes/utils/) (`8-mapper-streams-artiste.py`, `9-mapper-streams-annee.py`, `10-reducer-somme.py`, `11-reducer-streams-hbase.py`, `12-prepare-streams-tsv.py`, `13-graph-streams-hbase.py`) ; énoncés des exercices dans [`../exercices/04-mapreduce.md`](../exercices/04-mapreduce.md) et suivants.

> ✨ **Principe** : chaque étape ci-dessous est celle d'un exercice déjà vu. **Rien de nouveau n'est à inventer : chaque commande a déjà été faite.** En cas de doute, le lien vers l'exercice d'origine est donné à côté. Ce qui change, c'est la **question** posée à la donnée — pas la chaîne qui y répond.

---

## II. HDFS — accueillir le fichier

> Exercice d'origine : [chapitre 03, III.E — Mise en pratique](../03-hdfs.md#e-mise-en-pratique--le-csv-spotify).

**1.** Au chapitre 03, vous avez déjà déposé ce fichier dans HDFS. Vérifiez qu'il y est toujours ; sinon, déposez-le :

```bash
# depuis la machine Docker (pas le conteneur)
docker cp data/Spotify_Most_Streamed_Songs.csv hadoop-master:/home/

# dans le conteneur hadoop-master
hdfs dfs -mkdir -p input
hdfs dfs -put Spotify_Most_Streamed_Songs.csv input/
hdfs dfs -ls input
```

**Résultat attendu** : `input/Spotify_Most_Streamed_Songs.csv` — `155593` octets, réplication `1`.

**2.** Vérifiez que le fichier est complet : `hdfs dfs -cat input/Spotify_Most_Streamed_Songs.csv | tail -3` (la dernière ligne est la chanson `Alone` de `Burna Boy`, 96 007 391 streams).

> 📋 **Pour le rapport** : documentez la connexion à la machine (IP, port, utilisateur — le mot de passe ne se documente pas) telle qu'elle a réellement fonctionné, les services démarrés et les commandes utilisées (HDFS, YARN, ZooKeeper, HBase). Chaque action du TP donne lieu à une ligne « action → retour observé ».

---

## III. Question 1 — le classement des artistes (MapReduce)

> Exercices d'origine : [`../exercices/04-mapreduce.md`](../exercices/04-mapreduce.md) (prise en main et exercices 1-3).

**Q1 — Quels artistes cumulent le plus de streams (toutes années confondues) ?**

**1.** Reprenez les squelettes fournis au TP :

- Mapper : [`../annexes/utils/8-mapper-streams-artiste.py`](../annexes/utils/8-mapper-streams-artiste.py) — émet `artiste<TAB>streams` (en-tête sauté par test de valeur, `int(streams)` protégé, artiste vide ignoré).
- Reducer : [`../annexes/utils/10-reducer-somme.py`](../annexes/utils/10-reducer-somme.py) — fait la somme par clé.

**2.** Testez **localement** avant le cluster :

```bash
cat data/Spotify_Most_Streamed_Songs.csv | python3 annexes/utils/8-mapper-streams-artiste.py | sort -t $'\t' -k1,1 | python3 annexes/utils/10-reducer-somme.py | sort -t $'\t' -k2,2nr | head -5
```

**Résultat attendu** :

```text
The Weeknd      14185552870
Taylor Swift    14053658300
Ed Sheeran      13908947204
Harry Styles    11608645649
Bad Bunny        9997799607
```

(`644` artistes au total, `952` lignes retenues sur 953 — la ligne 576, corrompue, est écartée par la protection — `489 458 828 542` streams tous artistes confondus.)

**3.** Lancez le job sur le cluster (services démarrés) :

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file annexes/utils/8-mapper-streams-artiste.py -mapper "python3 8-mapper-streams-artiste.py" \
  -file annexes/utils/10-reducer-somme.py -reducer "python3 10-reducer-somme.py" \
  -input input/Spotify_Most_Streamed_Songs.csv -output output/streams_artiste
hdfs dfs -cat output/streams_artiste/part-00000 | sort -t $'\t' -k2,2nr | head -5
```

**Résultat attendu** : les mêmes valeurs que le test local (le cluster ne change *rien* au calcul, il le distribue).

> ⚠️ Rappel du chapitre 04 : la sortie ne doit **pas exister** avant le job (`-rm -r -f` si besoin).

> 📋 **Pour le rapport** : décrivez la structure d'un job MapReduce (mapper, reducer) et les commandes de soumission réelles, avec le **retour** observé (le `number of splits` du log, le nombre de mappers/reducers). Détaillez les protections du mapper (en-tête par valeur, `int()` protégé, artiste vide) et **testez le comportement** de la ligne 576 : que se passe-t-il si on retire le `try/except` du mapper ?

---

## IV. Question 2 — la clé composite artiste#année (MapReduce)

> Exercice d'origine : [`../exercices/04-mapreduce.md`](../exercices/04-mapreduce.md) — le geste de la clé composite.

**Q2 — Et en y regardant année par année ? Quel artiste·année cumule le plus de streams ?**

**1.** Utilisez le mapper de clé composite : [`../annexes/utils/9-mapper-streams-annee.py`](../annexes/utils/9-mapper-streams-annee.py) — émet `artiste#année<TAB>streams`, en filtrant les années invalides (pas 4 chiffres).

**2.** Testez localement, puis lancez le job :

```bash
cat data/Spotify_Most_Streamed_Songs.csv | python3 annexes/utils/9-mapper-streams-annee.py | sort -t $'\t' -k1,1 | python3 annexes/utils/10-reducer-somme.py | sort -t $'\t' -k2,2nr | head -5
```

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file annexes/utils/9-mapper-streams-annee.py -mapper "python3 9-mapper-streams-annee.py" \
  -file annexes/utils/10-reducer-somme.py -reducer "python3 10-reducer-somme.py" \
  -input input/Spotify_Most_Streamed_Songs.csv -output output/streams_artiste_annee
```

**Résultat attendu** :

```text
Bad Bunny#2022         7436274050
Olivia Rodrigo#2021    7302144942
Ed Sheeran#2017        6122072964
Harry Styles#2022      6016671168
Harry Styles#2019      5591974481
```

`754` paires `artiste#année` au total. Comparez avec Q1 : `The Weeknd` est premier **toutes années confondues**, mais aucune de ses années n'apparaît dans ce top — c'est ce que la clé composite révèle : les sommets **par année** ne sont pas les mêmes artistes que les sommets **cumulés**.

> 💡 **Lien pédagogique** : la clé est **composite** — deux dimensions accolées dans une seule clé textuelle. Le reducer la voit comme une valeur unique ; c'est le même geste que la clé composite du module précédent.

> 📋 **Pour le rapport** : expliquez pourquoi la clé composite passe par UNE seule clé (et le tri du shuffle), et notez ce que la question 2 apporte de plus que la question 1.

---

## V. Question 3 — le même résultat en SQL (Hive)

> Exercice d'origine : [`../exercices/05-hive.md`](../exercices/05-hive.md).

**Q3 — Le classement de Q1, mais en SQL : MR et Hive sont-ils d'accord ?**

**1.** Préparez le TSV (script [`../annexes/utils/12-prepare-streams-tsv.py`](../annexes/utils/12-prepare-streams-tsv.py)) — il retire l'en-tête, écarte la ligne corrompue et garde 4 colonnes `artiste · annee · titre · streams` :

```bash
python3 annexes/utils/12-prepare-streams-tsv.py data/Spotify_Most_Streamed_Songs.csv streams-clean.tsv
wc -l streams-clean.tsv          # 952 lignes de données (1 en-tête retiré, 1 ligne 576 écartée)
hdfs dfs -mkdir -p /user/root/spotify/clean
hdfs dfs -put streams-clean.tsv /user/root/spotify/clean/
```

**2.** Dans Hive (`./start-hive.sh`, `beeline -u jdbc:hive2://localhost:10000`), créez la table externe et interrogez :

```sql
CREATE DATABASE IF NOT EXISTS spotify_db;
CREATE EXTERNAL TABLE spotify_db.chansons (artiste STRING, annee INT, titre STRING, streams BIGINT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '/user/root/spotify/clean';

SELECT COUNT(*) FROM spotify_db.chansons;          -- 952
SELECT artiste, SUM(streams) AS total FROM spotify_db.chansons
  GROUP BY artiste ORDER BY total DESC LIMIT 5;    -- The Weeknd 14185552870 …
SELECT artiste, annee, SUM(streams) AS total FROM spotify_db.chansons
  GROUP BY artiste, annee ORDER BY total DESC LIMIT 5;  -- Bad Bunny # 2022 : 7436274050 …
```

**Résultat attendu** : `COUNT(*)` = **952** ; le top 5 des artistes est identique à Q1 ; le top artiste·année est identique à Q2.

> ✨ **Lien pédagogique** : comparez le `part-00000` de Q1/Q2 et les deux dernières requêtes : **mêmes lignes** ? C'est la validation croisée du TD-02, refaite avec votre donnée.

> 📋 **Pour le rapport** : montrez la requête Hive qui reproduit l'agrégat de votre job, et commentez la comparaison (lignes de code, lisibilité, EXPLAIN). Tracez une commande `EXPLAIN SELECT …` et notez le retour dans le rapport.

---

## VI. Question 4 — stocker le résultat en clé/valeur (HBase)

> Exercice d'origine : [`../exercices/06-hbase.md`](../exercices/06-hbase.md) — partie IV (exercices 4-5).

**Q4 — Comment retrouver, en clé/valeur, le total de streams d'un artiste ?**

**1.** Démarrez HBase + Thrift et créez la table **avant** le job (jamais dans le reducer) :

```bash
./start-hbase.sh && ./start-thrift.sh
hbase shell
  > create 'artistes', 'stat'
  > exit
```

**2.** Relancez Q1 avec un reducer qui **écrit dans HBase** (rowkey = artiste) : [`../annexes/utils/11-reducer-streams-hbase.py`](../annexes/utils/11-reducer-streams-hbase.py)

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file annexes/utils/8-mapper-streams-artiste.py -mapper "python3 8-mapper-streams-artiste.py" \
  -file annexes/utils/11-reducer-streams-hbase.py -reducer "python3 11-reducer-streams-hbase.py" \
  -input input/Spotify_Most_Streamed_Songs.csv -output output/streams_hbase
```

**3.** Vérifiez par `get` / `scan` (le `put` ne confirme rien) :

```bash
hbase shell
  > count 'artistes'                -- 644 artistes
  > get 'artistes','The Weeknd'     -- stat:total => 14185552870
```

**Résultat attendu** : `count` = **644** ; `get 'artistes','The Weeknd'` affiche `stat:total      timestamp=…, value=14185552870`. 14 185 552 870 = le total de The Weeknd trouvé en Q1 : la cohérence entre les moteurs se vérifie.

> ⚠️ Le `11-reducer-streams-hbase.py` n'écrit **rien sur stdout** (un `print` corromprait le `part-00000`) — les messages de suivi partent sur `stderr`.

> 📋 **Pour le rapport** : expliquez pourquoi ranger l'**artiste en rowkey** (et pas la chanson) sert la question, retrouvez la valeur de The Weeknd par `get`, et commentez ce que le `count` vous apprend sur le volume de la table.

---

## VII. Question 5 — ingérer au lieu d'un `put` (NiFi)

> Exercice d'origine : [`../exercices/07-nifi.md`](../exercices/07-nifi.md).

**Q5 — Et si un flux déposait le fichier à notre place ?**

**1.** Déposez le CSV dans le dossier d'entrée du flux :

```bash
cp data/Spotify_Most_Streamed_Songs.csv nifi/input/
```

**2.** Dans NiFi (`https://localhost:8443/nifi`), vérifiez que le flux du chapitre 07 tourne : **GetFile → ConvertRecord → PutFile**, avec le câblage `failure` vers `/opt/nifi/errors`. Le fichier doit ressortir en **TSV** dans `/opt/nifi/output` (= `/home/staging` du master) — l'extension reste `.csv`, c'est le contenu qui change (cf. chap. 07).

**3.** Déposez le fichier produit dans HDFS, là où Hive le lit :

```bash
docker exec -it hadoop-master bash
hdfs dfs -moveFromLocal /home/staging/Spotify_Most_Streamed_Songs.csv /user/root/spotify/clean/
```

**4.** Re-compter dans Hive pour prouver que l'ingestion a fonctionné :

```sql
SELECT COUNT(*) FROM spotify_db.chansons;   -- 1906 (952 + les 954 lignes du flux)
SELECT artiste, SUM(streams) AS total FROM spotify_db.chansons
  GROUP BY artiste ORDER BY total DESC LIMIT 5;   -- The Weeknd toujours 14185552870
```

**Résultat attendu** : le compteur **augmente** de 954 lignes (1 en-tête + 953 chansons — le flux ne filtre rien, la ligne 576 passe) ; le top 5, lui, **ne bouge pas**.

> 🎯 **Piège à observer — le schema-on-read** : le TSV qui sort du flux a **25 colonnes** (le schéma Spotify complet), alors que la table `chansons` n'en déclare que **4**. Hive ne vérifie rien : il lit les quatre premiers champs de chaque ligne. Dans ces lignes-là, la colonne `streams` de la table reçoit… l'année de sortie (≈ 2023) — des valeurs mille millions de fois trop petites pour troubler le top 5. Seul le `COUNT(*)` trahit l'intrus. Repérez, dans **Data Provenance**, le lineage du fichier ingéré (GetFile → ConvertRecord → PutFile → …) : c'est la trace « qui, quand, quoi, où » demandée par ce TP.

> 📋 **Pour le rapport** : documentez l'alternative flux NiFi plutôt que le `put` manuel, ce que le flux ne fait pas tout seul (l'en-tête conservé, aucune ligne filtrée), et qui surveille cette étape.

---

## VIII. Visualisation — le top 10 des artistes

**1.** Créez le graphique avec Matplotlib : un script python **séparé** qui lit HBase (via HappyBase) et génère le PDF du top 10 des artistes (référence : [`../annexes/utils/13-graph-streams-hbase.py`](../annexes/utils/13-graph-streams-hbase.py)).

```bash
python3 annexes/utils/13-graph-streams-hbase.py > top10.txt
```

**Résultat attendu** : `The Weeknd 14185552870` en tête dans `top10.txt` et un `resultat.pdf` avec les 10 barres.

> ⚠️ Le script tourne **sur le master**, hors MapReduce, Thrift démarré (jarre du chapitre 06). `matplotlib.use('Agg')` avant l'import de `pyplot` est obligatoire sans écran.

**2.** Récupérez les sorties du job MapReduce et le PDF vers votre poste local :

```bash
hdfs dfs -get output/streams_artiste/part-00000 ./resultats.txt
docker cp hadoop-master:/home/resultat.pdf ./
docker cp hadoop-master:/home/resultats.txt ./
```

---

## Bilan — la chaîne complète

Vous venez d'enchaîner, avec **une seule donnée**, les cinq étapes de la pipeline :

| Étape | Outil | Rôle dans la chaîne | Vérification |
|---|---|---|---|
| Dépôt | HDFS | stocker le fichier (blocs, réplication 1) | `ls` / `cat \| tail` |
| Calcul | MapReduce | Q1 Q2 : sommer les streams par artiste / artiste·année | `part-00000` |
| SQL | Hive | Q3 : même agrégat en SQL (validation croisée) | `COUNT(*)` = 952 |
| Clé/valeur | HBase | Q4 : totaux par artiste (rowkey = artiste) | `get` / `count` = 644 |
| Ingestion | NiFi | Q5 : déposer le fichier sans `put` manuel | `COUNT(*)` = 1906 |

```bash
# emplacement des sorties de cette session
output/streams_artiste/part-00000           # MapReduce Q1
output/streams_artiste_annee/part-00000     # MapReduce Q2
/user/root/spotify/clean/                   # table externe Hive (LOCATION)
artistes                                    # table HBase (scan / get)
```
> ⚠️ La localisation Hive est celle définie dans `CREATE EXTERNAL TABLE … LOCATION` : reportez celle **que vous avez réellement utilisée**.

---

## Bonus — le changement de dimension

Le pipeline **ne change pas** ; seule la **clé du mapper** change. Modifiez le mapper 8 pour émettre le **titre** (colonne 0) au lieu de l'artiste (colonne 1) — une ligne — et relancez Q1 avec le même reducer :

```bash
cat data/Spotify_Most_Streamed_Songs.csv | python3 <votre-mapper-titres>.py | sort -t $'\t' -k1,1 | python3 annexes/utils/10-reducer-somme.py | sort -t $'\t' -k2,2nr | head -5
```

**Résultats attendus** :

```text
Blinding Lights                            3703895074
Shape of You                               3562543890
Someone You Loved                          2887241814
Dance Monkey                               2864791672
Sunflower - Spider-Man: Into the Spider-Verse   2808096550
```

`942` titres distincts. Notez le contraste avec Q1 : `Blinding Lights` est LA chanson la plus streamée, mais The Weeknd ne doit son classement que l'addition de **toutes** les siennes — la dimension choisie change la réponse, pas la chaîne.

> **Retours à noter** : quelles lignes de code avez-vous réellement dû toucher ? C'est la démonstration du contrat clé/valeur : changez la clé, tout le reste roule.

---

## Livrables attendus

Le rendu est une **documentation des actions et de leurs retours**, enrichie des résultats observés :

1. **La documentation pas à pas** : pour chaque étape (Q1 → Q5), le **contexte**, la **commande lancée**, le **retour obtenu** (copie d'écran ou transcription), et la **conclusion** — y compris les erreurs rencontrées et corrigées.
2. **Le code source** de vos jobs MapReduce (ou le nom des scripts `../annexes/utils` réutilisés et pourquoi).
3. **Les résultats** : `part-00000` de Q1/Q2, requêtes Hive et comparaison MR ↔ Hive, `get`/`scan` HBase, et le `top10.txt`.
4. **Le graphique** Matplotlib (au format PDF).
5. **La matrice de contrôle** : pour chacun des cinq outils, **qui ingère ? qui déplace ? qui stocke ? qui calcule ? qui sert ?** — l'ossature du rapport, comme posée en question de contrôle.
