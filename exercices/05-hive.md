![Diginamic Logo](../assets/diginamic.png) ![Hadoop Logo](../assets/hadoop.png)

# Exercice 05 : Hive

> **Type** : exercice guidé (parties I-II) + exercices autonomes (partie III)
> **Prérequis** : séquence 05 (Hive), EX 04 (MapReduce) — on réutilise les données Spotify
> **Durée estimée** : 1 h 30 (parties I-II) + 45 min à 1 h (partie III)

Cet exercice est la **contrepartie SQL** de l'EX 04 (MapReduce) : vous aviez écrit des jobs Python pour questionner les données Spotify, vous allez maintenant écrire des requêtes — et vérifier que les résultats correspondent.

_📌 Avant de commencer, relisez la section [« Premiers pas sur le cluster »](../05-hive.md#premiers-pas-sur-le-cluster) du cours : le point de départ est identique._

## Table des matières

- [Partie I — Prise en main](#partie-i--prise-en-main)
- [Partie II — Interroger en SQL](#partie-ii--interroger-en-sql)
- [Partie III — Exercices autonomes](#partie-iii--exercices-autonomes)

---

## Partie I — Prise en main

### 1. Lancer Hive via Beeline

Dans le master (conteneur `hadoop-master`), démarrez d'abord HiveServer2 si ce n'est pas déjà fait :

```bash
./start-hive.sh
```

Puis connectez-vous avec **Beeline**, le seul client SQL de Hive 4 :

```bash
beeline -u jdbc:hive2://localhost:10000
```

> ⚠️ **Si `beeline: command not found`** : le `PATH` de l'image n'est exporté que pour certains types de shell. Deux sorties, au choix :
> `export PATH=$PATH:/opt/hive/bin` puis `which beeline`, ou le chemin complet `/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000`.
>
> Et **lançez Beeline depuis `hadoop-master`** : le Metastore est une base Derby embarquée, attachée à ce conteneur. Depuis un esclave, l'URL `localhost:10000` ne désigne rien.
>
> **Et la commande `hive` ?** Le binaire existe encore et son `--help` liste même un service `cli` *par défaut* — mais ce n'est plus qu'un wrapper qui redirige vers Beeline. La CLI historique a bel et bien disparu : la classe `CliDriver` n'est dans aucun jar de Hive 4. En pratique, `hive -e "…"` se termine en code 1 sur `Cannot run commands specified using -e. No current connection`. Beeline est la seule voie — tapez-la directement.

**Résultat attendu** : le prompt `0: jdbc:hive2://localhost:10000>` remplace le prompt shell. La première connexion peut prendre quelques dizaines de secondes (initialisation du Metastore Derby). `!quit` pour sortir.

```sql
SHOW DATABASES;
```

**Résultat attendu** : au moins `default`.

### 2. Créer la base de travail

```sql
CREATE DATABASE spotify_db;
USE spotify_db;
SHOW TABLES;
```

**Résultat attendu** : `OK` à chaque commande ; `SHOW TABLES` affiche vide (aucune table dans cette base).

### 3. Préparer les données : du CSV au TSV

Le CSV Spotify contient des champs quotés avec virgules internes (`"Latto, Jung Kook"`) : un séparateur virgule casserait ces lignes. On prépare donc un **TSV** (tabulations) que Hive lira sans ambiguïté.

Sur votre machine locale (ou dans le conteneur, le script est fourni) :

```bash
python3 annexes/utils/7-prepare-spotify-tsv.py data/Spotify_Most_Streamed_Songs.csv spotify-clean.tsv
```

**Résultat attendu** : 954 lignes (header + 953 titres), 25 colonnes. Vérifiez :

```bash
wc -l spotify-clean.tsv
```

Puis poussez le fichier dans HDFS (cf. séquence 03) :

```bash
hdfs dfs -mkdir -p /user/root/spotify/clean
hdfs dfs -put spotify-clean.tsv /user/root/spotify/clean/
hdfs dfs -ls /user/root/spotify/clean
```

**Résultat attendu** : `spotify-clean.tsv` apparaît dans le listing HDFS.

### 4. Déclarer la table externe

De retour dans Beeline :

```sql
USE spotify_db;

CREATE EXTERNAL TABLE spotify (
  track_name STRING,
  artist_name STRING,
  artist_count INT,
  released_year INT,
  released_month INT,
  released_day INT,
  in_spotify_playlists INT,
  in_spotify_charts INT,
  streams BIGINT,
  in_apple_playlists INT,
  in_apple_charts INT,
  in_deezer_playlists STRING,
  in_deezer_charts INT,
  in_shazam_charts STRING,
  bpm INT,
  `key` STRING,
  mode STRING,
  danceability_pct DOUBLE,
  valence_pct DOUBLE,
  energy_pct DOUBLE,
  acousticness_pct DOUBLE,
  instrumentalness_pct DOUBLE,
  liveness_pct DOUBLE,
  speechiness_pct DOUBLE,
  cover_url STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/root/spotify/clean';
```

> ⚠️ **Deux pièges de typage, mesurés sur ces données :**
>
> - `key` est un mot réservé SQL : backticks obligatoires. (`mode` **ne l'est pas** en Hive 4 — testé, la DDL passe sans backticks.)
> - `in_deezer_playlists` est **déclaré `STRING`** alors qu'il contient des nombres : **79 valeurs sont écrites `1,003`** (milliers séparés par une virgule, résidu d'un export tableur). Déclarée `INT`, la colonne aurait mis ces 79 valeurs à `NULL` **sans le moindre avertissement**, et `MAX(in_deezer_playlists)` aurait renvoyé `974` — un maximum **faux**, car les plus grandes valeurs sont justement parmi celles perdues. C'est le type de corruption silencieuse qu'aucun compteur ne signale.
> - `streams` est `BIGINT` : certaines valeurs dépassent INT (le maximum mesuré est 3 703 895 074).

### 5. Contrôler que tout est en place

```sql
SELECT COUNT(*) FROM spotify;
```

**Résultat attendu** : **`954`** avec la DDL du I.4. Trois valeurs sont possibles, et c'est la question :

| Vous obtenez | Parce que |
|---|---|
| **954** | la DDL de l'EX 05, telle quelle : le fichier garde son en-tête et Hive ne le saute pas |
| **953** | vous avez ajouté `TBLPROPERTIES ('skip.header.line.count'='1')`, ou retiré l'en-tête du fichier |
| **952** | `COUNT(streams)` et non `COUNT(*)` : deux lignes ont un `streams` non numérique mis à `NULL` par Hive — l'en-tête (`streams='streams'`) et la ligne 576 du fichier source (champ corrompu) |

> 🤔 **Question de contrôle** : `wc -l` annonce 954 lignes et `COUNT(*)` renvoie aussi 954. D'habitude on promet 953 : « le compteur ne compte pas l'en-tête ». Pourquoi n'est-ce pas le cas ici, et qu'est-ce que ça change pour tout ce que vous allez compter ensuite ? _Réfléchissez avant de regarder le corrigé — la réponse tient en une ligne, et elle explique pourquoi Hive ne « plante » jamais sur un en-tête : il en fait une ligne de données._

---

## Partie II — Interroger en SQL

### 1. Le top 10 — validation croisée avec MapReduce

```sql
SELECT artist_name, SUM(streams) AS total
FROM spotify
GROUP BY artist_name
ORDER BY total DESC
LIMIT 10;
```

**Résultat attendu** : une table de 10 lignes. Les six premières, mesurées, sont :

```
The Weeknd      14185552870
Taylor Swift    14053658300
Ed Sheeran      13908947204
Harry Styles    11608645649
Bad Bunny        9997799607
Olivia Rodrigo   7442148916
```

Total de la colonne `streams` de la table (952 valeurs, la ligne corrompue passée à `NULL`) : **489 458 828 542** streams.

> ⚠️ **Ces valeurs supposent la convention de l'exercice** : `GROUP BY artist_name` regroupe la colonne **telle quelle**, donc `"Latto, Jung Kook"` forme une clé unique et un duo ne contribue à aucun de ses deux interprètes solo. **Ce n'est pas la convention de l'EX 04** : là, c'est le *mapper* que vous avez écrit qui décide, et s'il éclate les duos le classement change — mesuré, The Weeknd passe à **23 929 760 757** et Bad Bunny à **23 813 527 270** (c'est par **nombre de titres** — 40 pour Bad Bunny, 38 pour Taylor Swift, 37 pour The Weeknd — que la version éclatée se distingue).
>
> 🤔 **Question de contrôle — la plus importante de la semaine** : la validation croisée MR ↔ Hive n'a de sens que si **les deux côtés appliquent la même convention**. Écrivez la requête Hive qui éclate les duos, et la requête MapReduce qui ne les éclate pas : vous devez retrouver, de part et d'autre, les deux classements ci-dessus. Si vos chiffres MR et Hive divergent, la première cause à écarter n'est pas un bug — c'est une divergence de convention.

> 🤔 **Validation croisée** : dans [`04-mapreduce.md`](04-mapreduce.md), l'exercice 1 calculait les streams **par année de sortie**. La requête équivalente ici serait `GROUP BY released_year`. Lancez-la et comparez ligne à ligne avec votre sortie MapReduce : **ce sont exactement les mêmes chiffres** — c'est la preuve que Hive compile le SQL en MapReduce sous le capot.

**Résultat attendu** (extrait de trois années de contrôle) :

```
1975    4206105352
1984    3471625366
2023    25808484104
```

Ces trois valeurs ont été recalculées deux fois indépendamment (Python avec quoting CSV, puis `SUM()` dans Hive) et concordent. Si vous trouvez **exactement 4 206 105 352** pour 1975, votre pipeline est juste. Si vous trouvez un chiffre proche mais pas égal, comparez `COUNT(*)` et `COUNT(streams)` : la ligne corrompue du fichier source est probablement la seule cause, et c'est précisément ce que la question de contrôle cherche à vous faire découvrir.

### 2. Moyenne de danceability par année

```sql
SELECT released_year, AVG(danceability_pct) AS dance_moyenne
FROM spotify
GROUP BY released_year
ORDER BY released_year;
```

**Résultat attendu** : une ligne par année, valeurs entre 0 et 100 (le champ est un pourcentage).

### 3. Partitions logiques

Créez une table partitionnée, puis requêtez avec filtre de partition :

```sql
CREATE TABLE spotify_part (
  track_name STRING,
  artist_name STRING,
  streams BIGINT
  -- … schéma réduit pour l'exercice
)
PARTITIONED BY (released_year INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE spotify_part PARTITION (released_year)
SELECT track_name, artist_name, streams, released_year
FROM spotify;
```

**Résultat attendu** : `SHOW PARTITIONS spotify_part;` liste **50 partitions, de 1930 à 2023** — pas « environ 1975 à 2023 ».

> 🤔 **Question de contrôle** : vous voyez peut-être **51** entrées, dont une `__HIVE_DEFAULT_PARTITION__`. Aucune donnée n'a d'année vide — le fichier compte bien 50 années distinctes. D'où vient donc cette 51ᵉ partition, et quel rapport avec le `COUNT(*)` du I.5 ? _Indice : regardez ce que vaut `released_year` pour la première ligne du fichier._

Si vous voulez 50 partitions et rien d'autre, excluez cette ligne de l'`INSERT` :

```sql
INSERT OVERWRITE TABLE spotify_part PARTITION (released_year)
SELECT track_name, artist_name, streams, released_year
FROM spotify
WHERE released_year IS NOT NULL;
```

Puis observez l'élagage de partitions sur le plan :

```sql
EXPLAIN
SELECT COUNT(*) FROM spotify_part WHERE released_year = 2022;
```

**Résultat attendu** : le plan ne mentionne **que** la partition `released_year=2022` — les autres dossiers ne sont pas lus.

### 4. DROP TABLE — la leçon clé

Vérifiez d'abord le contenu HDFS, puis supprimez la table et regardez ce qui reste :

```bash
# dans le shell (sortez de Beeline avec !quit) :
hdfs dfs -ls /user/root/spotify/clean
```

```sql
-- dans Beeline :
DROP TABLE spotify;
```

```bash
# dans le shell :
hdfs dfs -ls /user/root/spotify/clean
```

**Résultat attendu** : le fichier `spotify-clean.tsv` **est toujours là**. La table externe est supprimée du Metastore (les métadonnées), mais HDFS reste maître de ses données. Recréez la table (commande du I.4) pour la suite de l'exercice.

### 5. EXPLAIN — retrouver le MapReduce

```sql
EXPLAIN
SELECT artist_name, SUM(streams) AS total
FROM spotify
GROUP BY artist_name;
```

**Résultat attendu** : le plan affiche des **stages** (Map/Reduce) : le map lit les lignes, le shuffle regroupe par `artist_name`, le reduce agrège — le mapper et le reducer que vous écriviez en Python à l'EX 04, générés automatiquement.

---

## Partie III — Exercices autonomes

Les corrigés sont dans [`corriges/05-hive.md`](corriges/05-hive.md) — cherchez avant de regarder.

### Exercice 1 : Top 5 artistes par nombre de titres

Écrire la requête qui classe les artistes par nombre de titres présents dans le dataset (attention aux artistes multiples dans un champ : on reste au niveau simple — une ligne = un titre).

**Résultat attendu** : 5 lignes `artist_name  nombre_de_titres`. En comptant la colonne artiste **telle quelle** (une ligne = un titre, un duo = un « artiste ») :

```
Taylor Swift    34
The Weeknd      22
Bad Bunny       19
SZA             19
Harry Styles    17
```

> 🤔 **Question de contrôle** : refaites le comptage en **éclatant** les duos sur chaque artiste (`split` + `explode`, hors périmètre ici — mentionnez la méthode). Le classement change, et Bad Bunny passe en tête avec **40** titres. Pourquoi le même dataset et la même question donnent-ils deux réponses justes et différentes, et laquelle écririez-vous dans un rapport ?

### Exercice 2 : Streams moyens par année de sortie

Moyenne de `streams` groupée par `released_year`, triée par année croissante.

**Résultat attendu** : une ligne par année `année  moyenne_streams`. C'est l'exercice 2 de l'EX 04 (MapReduce) en SQL : vous y accumuliez somme et compte, la moyenne se calcule à la fin de chaque groupe. Comparez les deux sorties — même plage 1930 → 2023, mêmes ordres de grandeur.

### Exercice 3 : Quand Hive plutôt que MapReduce ?

Question de réflexion, réponse en 3-5 lignes dans votre rapport de TP : votre équipe doit (a) explorer un dataset inconnu différent chaque semaine, (b) produire chaque nuit un rapport agrégé avec un SQL de 20 lignes déjà validé par le métier, (c) parser un format binaire propriétaire avec une machine à états de 300 lignes de logique. Pour chaque cas, Hive ou job MapReduce à la main ? Justifiez en une phrase par cas.

**Résultat attendu** : (a) Hive — l'exploration ad-hoc est le cœur du SQL ; (b) Hive aussi — un SQL de 20 lignes validé relance chaque nuit via `beeline -f rapport.sql` reste simple à maintenir ; le MapReduce à la main se justifie quand la logique métier est complexe (formats exotiques, enchaînements d'états, optimisations au niveau enregistrement).

---

## Pour aller plus loin

- **Mode scripté** : Beeline sait exécuter un fichier de requêtes — `beeline -u jdbc:hive2://localhost:10000 -f rapport.sql` — l'équivalent du `hive -f` d'antan, pratique pour l'exercice 3.
- **UI HiveServer2** : l'interface web du service est sur le port 10002 (sessions ouvertes, requêtes en cours).
- **Lien avec la séquence 07 (NiFi)** : ce fichier TSV dans HDFS, c'est celui que le flux NiFi de l'EX 07 saura déposer automatiquement — la même transformation CSV→TSV réalisée en flux plutôt qu'en script.
- **Lien avec la séquence 06 (HBase)** : la table Hive et la table HBase de l'EX 06 peuvent référencer les mêmes données — comparez les modèles (colonnes larges SQL vs familles de colonnes clairsemées).
