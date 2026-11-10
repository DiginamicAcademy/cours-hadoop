[← Chapitre précédent](04-mapreduce-et-yarn.md) · [Sommaire](Readme.md) · [Chapitre suivant →](06-hbase-et-zookeeper.md)

![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# **Hive, l'entrepôt SQL sur Hadoop**

> **Objectifs** : comprendre ce qu'apporte Hive par rapport à MapReduce · maîtriser l'architecture (HiveServer2, Metastore, driver) · distinguer tables externes et managées · écrire des requêtes SQL sur des données HDFS · relier SQL et MapReduce via EXPLAIN.
> **Prérequis** : séquences 03-04 (HDFS, MapReduce) · **Durée** : ~4 h · **Type** : cours + manipulations

## **Sommaire**

* [Hive](#hive)
  * [Le problème : interroger sans programmer](#le-problème--interroger-sans-programmer)
  * [Présentation de Hive](#présentation-de-hive)
  * [Architecture de Hive](#architecture-de-hive)
  * [Le modèle de données](#le-modèle-de-données)
  * [Tables externes vs tables managées](#tables-externes-vs-tables-managées)
  * [SQL sur HDFS : la séparation calcul/stockage](#sql-sur-hdfs--la-séparation-calculstockage)
  * [Ce que compile Hive : le lien avec MapReduce](#ce-que-compile-hive--le-lien-avec-mapreduce)
  * [Premiers pas sur le cluster](#premiers-pas-sur-le-cluster)
  * [Hive en 2026 : datalake et lakehouse](#hive-en-2026--datalake-et-lakehouse)
  * [Hive, HBase, HDFS : quel stockage pour quel usage ?](#hive-hbase-hdfs--quel-stockage-pour-quel-usage-)

---

## **Hive**

---

### **Le problème : interroger sans programmer**

Après la séquence 04, vous savez écrire un job MapReduce en Python. Posez-vous maintenant la question du **quotidien d'une équipe data** :

- Le responsable marketing demande : _« Quels sont nos dix titres les plus streamés ? »_ → il faut écrire un mapper, un reducer, les tester, les lancer, récupérer la sortie.
- Une heure plus tard, il demande : _« Et la moyenne des streams par année de sortie ? »_ → nouveau mapper, nouveau reducer, nouveau cycle complet.
- Les analystes de l'équipe, eux, **ne connaissent ni Python ni Java** — et on ne peut pas leur demander d'apprendre MapReduce pour explorer des données.

MapReduce répond au **comment** exécuter un calcul distribué, pas au **quoi** calculer. Chaque question nouvelle impose un développement. Pour l'exploration de données — le cœur du travail analytique — il faut un langage **déclaratif** : on décrit le résultat voulu, la machine décide des étapes.

La solution historique de l'écosystème Hadoop pour ce problème : **Hive**, le SQL sur HDFS.

### **Présentation de Hive**

**Apache Hive** est un entrepôt de données (_data warehouse_) qui s'installe au-dessus de Hadoop et permet d'interroger les données stockées dans HDFS **avec un langage proche du SQL** : HiveQL.

Hive est né chez **Facebook en 2008**, avec exactement le problème décrit ci-dessus : des analystes qui savaient écrire du SQL, des données massives sur un cluster Hadoop, et aucun pont entre les deux. Facebook a donc écrit le traducteur : **du SQL vers des jobs MapReduce**. Le projet a rejoint Apache en 2009.

Concrètement, écrire :

```sql
SELECT artist_name, SUM(streams) AS total
FROM spotify
GROUP BY artist_name
ORDER BY total DESC
LIMIT 10;
```

...revient à lancer un job MapReduce complet — sans écrire une ligne de Python ni de Java. C'est la promesse de Hive : **le SQL des analystes, la puissance du cluster**.

> **Vocabulaire.** On dit que Hive est un _data warehouse_ (entrepôt de données) plutôt qu'une base de données : il ne stocke pas les données lui-même — elles restent des fichiers dans HDFS — et il est optimisé pour l'**analyse en batch** de gros volumes (OLAP), pas pour les transactions à la ligne (OLTP).

### **Architecture de Hive**

```
        ┌─────────────┐
        │   Analyste   │
        └──────┬──────┘
               │  HiveQL
        ┌──────▼──────────────────────────────┐
        │           HiveServer2                │
        │  ┌──────────────────────────────┐   │
        │  │ Driver : parse → compile →   │   │
        │  │ optimise → exécution         │   │
        │  └──────────┬───────────────────┘   │
        └─────────────┼───────────────────────┘
                      │
        ┌─────────────▼───────────┐     ┌──────────────────────┐
        │     Metastore           │◄───►│  Base relationnelle    │
        │ (catalogue : tables,     │     │  (Derby, MySQL…)      │
        │  schémas, partitions)   │     └──────────────────────┘
        └─────────────┬───────────┘
                      │
        ┌─────────────▼───────────────────────┐
        │   YARN → jobs MapReduce (calcul)    │
        │   HDFS  → fichiers (données)         │
        └─────────────────────────────────────┘
```

Trois briques à retenir :

* **HiveServer2** : le service qui reçoit les requêtes. On s'y connecte avec **Beeline** (client JDBC, interactif ou scripté). C'est lui qui héberge le **driver**. (Les vieilles CLI `hive` ont été retirées de Hive 4 — Beeline est le client officiel.)
* **Le driver** : le moteur de traduction. Il **parse** la requête, la **compile** en plan d'exécution (des jobs MapReduce — ou Tez), l'**optimise**, puis lance l'exécution sur **YARN**. C'est ici que le SQL devient du calcul distribué.
* **Le Metastore** : le **catalogue**. Il mémorise, dans une base relationnelle classique (Derby embarquée par défaut, MySQL en production) : _« la table `spotify` pointe vers ces fichiers HDFS, avec ces colonnes, ces types, ce format »_. **Point clé** : le Metastore ne contient **aucune donnée** — uniquement les métadonnées. Les données vivent dans HDFS.

> **Conséquence importante** : deux clients (Hive, mais aussi Spark SQL ou Trino s'ils sont configurés) peuvent interroger les **mêmes fichiers HDFS** via le même Metastore. Le stockage est partagé, seul le moteur de calcul change.

### **Le modèle de données**

Hive organise les données selon une hiérarchie familière pour qui connaît le SQL :

| Niveau | Rôle | Commande type |
|---|---|---|
| **Database** | espace de nommage logique | `CREATE DATABASE spotify_db;` |
| **Table** | schéma + localisation de fichiers | `CREATE EXTERNAL TABLE …` |
| **Partition** | sous-dossier = valeur d'une colonne | `PARTITIONED BY (released_year INT)` |
| **Bucket** | répartition en fichiers par hash | `CLUSTERED BY (artist) INTO 8 BUCKETS` |

* **Les partitions** traduisent une idée déjà vue en séquence 03 (l'arborescence HDFS) : partitioner par année de sortie revient à stocker `released_year=2015/`, `released_year=2022/`… Une requête filtrant sur une partition ne lit que le dossier concerné — c'est l'**élagage de partitions** (_partition pruning_), la première optimisation de Hive.
* **Les buckets** répartissent les lignes en un nombre fixé de fichiers selon le hash d'une colonne — utile pour jointures et échantillonnage.

**Point de vigilance** : Hive ne modifie pas les fichiers d'entrée. Il lit des fichiers **au format déclaré** (texte délimité, Parquet, ORC…) et suppose que la réalité correspond au schéma — il n'y a pas de contraintes d'intégrité comme dans MySQL. C'est à vous de garantir la cohérence entre le schéma déclaré et les données réelles.

### **Tables externes vs tables managées**

C'est **la** distinction à maîtriser — elle conditionne qui est maître des données.

| | Table **MANAGED** (managée) | Table **EXTERNAL** (externe) |
|---|---|---|
| Création | `CREATE TABLE …` | `CREATE EXTERNAL TABLE …` |
| Qui écrit les données | Hive (il les dépose dans HDFS) | **Vous** (n'importe quel outil : `hdfs dfs -put`, un job, NiFi…) |
| Cycle de vie | liée à Hive | liée à HDFS |
| `DROP TABLE` | supprime **les métadonnées ET les données HDFS** | supprime **uniquement les métadonnées** — les fichiers restent |

En pratique dans ce cours, nous utiliserons quasi exclusivement des **tables externes** : les données Spotify sont déposées dans HDFS (séquences 03-04), et Hive vient simplement déclarer un schéma **par-dessus**. HDFS reste le maître du stockage ; Hive n'est qu'une lentille de lecture.

```sql
-- Table externe : Hive ne "possède" rien
CREATE EXTERNAL TABLE spotify_clean (
  track_name STRING,
  artist_name STRING,
  streams BIGINT
  -- …
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/root/spotify/clean';
```

> **Réflexe de survie** : avant tout `DROP TABLE`, demander — est-ce une EXTERNAL ou une MANAGED ? Sur une table managée, DROP détruit des heures de pipeline. Vous verrez cette manipulation contrôlée en EX ([exercices/05](exercices/05-hive.md)).

### **SQL sur HDFS : la séparation calcul/stockage**

Reprenons le schéma de l'architecture : le Metastore sait _où_ sont les données (fichiers HDFS) et _comment_ les lire (schéma, format) ; YARN exécute le calcul. **Aucune donnée ne transite par Hive.**

Cette séparation est le concept le plus durable de l'écosystème — tous les moteurs modernes l'exploitent :

- Vous déposez un fichier dans HDFS avec `hdfs dfs -put` (séquence 03) ;
- Un job MapReduce en produit un autre (séquence 04) ;
- **Hive** interroge l'un ou l'autre en SQL (cette séquence) ;
- **Spark SQL** ou **Trino** pourraient interroger exactement les mêmes fichiers via le même Metastore — sans copie, sans conversion.

C'est la promesse du _lakehouse_ actuel : un stockage de fichiers ouvert (HDFS, puis S3, Iceberg), plusieurs moteurs de calcul à la demande.

### **Ce que compile Hive : le lien avec MapReduce**

Vous savez (séquence 04) ce qu'est un job MapReduce. Regardez maintenant ce que Hive fait de votre SQL — la boucle se referme :

```sql
EXPLAIN
SELECT artist_name, SUM(streams) AS total
FROM spotify_clean
GROUP BY artist_name;
```

La sortie du `EXPLAIN` décrit les phases du job généré, avec un vocabulaire qui doit vous être familier :

```
STAGE DEPENDENCIES:
  Stage-1 … (Map + Reduce)
  Stage-0 … (Fetch)
```

* Le **map** lit chaque ligne et émet une paire `(artiste, streams)` — exactement le mapper que vous avez écrit en Python à l'EX 04 ;
* le **shuffle** regroupe les paires de même clé — le travail du framework entre map et reduce (séquence 04) ;
* le **reduce** agrège — votre reducer.

**Une requête SQL de 6 lignes = un job MapReduce complet** (mapper, tri, reducer) que vous auriez écrit en une centaine de lignes de Python. C'est toute la valeur de Hive : pas un nouveau moteur de calcul, mais un **compilateur** qui génère du MapReduce pour vous.

> **Quand Hive, quand MapReduce à la main ?** Hive excelle pour l'**exploration ad-hoc** (question → requête) et les ETL déclaratifs. On retombe sur MapReduce (ou Spark) quand il faut un contrôle fin du traitement : logique métier complexe, formats exotiques, optimisations au niveau enregistrement. La séquence 04 vous a appris ce que fait le moteur ; Hive vous fait gagner le temps de l'écrire.

### **Premiers pas sur le cluster**

Hive **4.1.0** est installé dans l'image du cluster (`hadoop-cluster-base`) : HiveServer2 et le Metastore (Derby embarqué) sont préconfigurés — rien à installer. Son démarrage est un simple script dans le master :

```bash
./start-hive.sh     # lance HiveServer2 (port JDBC 10000) et attend qu'il réponde
```

Puis on s'y connecte avec **Beeline**, le client officiel (la CLI `hive` n'existe plus depuis Hive 4) :

```bash
beeline -u jdbc:hive2://localhost:10000
```

> La connexion affiche `0: jdbc:hive2://localhost:10000>` comme prompt — les requêtes qui suivent s'y saisissent (une par ligne, `;` pour terminer, `!quit` pour sortir).

```sql
SHOW DATABASES;
-- résultat attendu : default (+ vos bases créées)

CREATE DATABASE spotify_db;
USE spotify_db;

SHOW TABLES;
-- résultat attendu : vide — on créera la table Spotify à l'EX 05
```

> **Manipulations complètes** (création de la table Spotify, requêtes, EXPLAIN, DROP) : [exercices/05](exercices/05-hive.md). Le corrigé se trouve dans [`exercices/corriges/05-hive.md`](exercices/corriges/05-hive.md).

### **Hive en 2026 : datalake et lakehouse**

L'encart « écosystème historique » du cours 01 cite Hive parmi les briques toujours actives — le détail :

* **Hive 4.x** (4.0 en 2024, c'est la génération de notre cluster — 4.1.0) : intégration native d'**Apache Iceberg** (format de table lakehouse — transactions ACID, évolution de schéma, time travel), support d'**Apache Ranger** (sécurité fine), vues matérialisées améliorées, HiveServer2 consolidé.
* **Toujours massivement présent en entreprise** : les clusters on-premise legacy, Cloudera CDP, et les plateformes cloud qui en dérivent exécutent des quantités de SQL Hive en production chaque jour.
* **Concurrence** : pour le SQL interactif rapide, **Spark SQL** et **Trino/Presto** ont pris le dessus sur les nouveaux projets — mais ils partagent le même modèle (Metastore + fichiers) que Hive a popularisé. Apprendre Hive, c'est apprendre le modèle.

Positionnement dans notre cursus : Hive s'insère **entre MapReduce (04) et HBase (06)** — trois réponses à trois questions de stockage/interrogation sur le même HDFS.

### **Hive, HBase, HDFS : quel stockage pour quel usage ?**

Synthèse pour fixer les idées avant la séquence 06 :

| | HDFS (séq. 03) | Hive (cette séquence) | HBase (séq. 06) |
|---|---|---|---|
| **Nature** | système de fichiers distribué | entrepôt SQL par-dessus HDFS | base NoSQL orientée colonnes |
| **Accès** | commandes shell, MapReduce | SQL (HiveQL) | clé de ligne, scans (shell, API) |
| **Latence** | minutes (batch) | dizaines de secondes à minutes | **millisecondes** (temps réel) |
| **Modèle** | fichiers, blocs | tables, partitions | tables larges clairsemées |
| **Usage type** | stocker des archives, gros fichiers | analyser, agréger, explorer | servir des lectures/écritures à la ligne |

Les trois cohabitent sur le même cluster — et même sur les mêmes données, via les tables externes.

---

[← Chapitre précédent](04-mapreduce-et-yarn.md) · [Sommaire](Readme.md) · [Chapitre suivant →](06-hbase-et-zookeeper.md)
