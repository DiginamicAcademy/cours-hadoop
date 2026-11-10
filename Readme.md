<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/Logo-diginamic-color-blk.png">
    <img src="assets/Logo-diginamic-color.svg" alt="Diginamic Formation" width="358">
  </picture>
</p>

# Exploiter l'écosystème Hadoop : HDFS, MapReduce, Hive, HBase et NiFi

## L'idée qui tient tout le cours

> **Hadoop n'est pas un outil, c'est un étage.** Avant de toucher aux promesses du Big Data, il faut comprendre l'étage de base : un fichier coupé en blocs, répliqué sur des machines ordinaires, calculé là où il repose. Le cours prend le temps de lire l'architecture (HDFS, YARN) avant de faire tourner quoi que ce soit, puis traverse les briques qui ont survécu à l'usage — MapReduce, Hive, HBase, NiFi — sans occulter celles qui n'ont pas survécu (Pig, Sqoop, Oozie) et ce qui les a remplacées.

Fil rouge des exercices : le jeu de données **Spotify** (`data/Spotify_Most_Streamed_Songs.csv`), le même du chapitre 03 au chapitre 07. Le **TP final** change de donnée — un jeu de ventes (`data/dataw_fro03_mini_1000.csv`, version complète en bonus) — pour que la chaîne soit refaite seule, sans filets.

## Objectifs pédagogiques

À l'issue du cours, vous saurez : situer Hadoop et son écosystème dans le Big Data d'aujourd'hui ; stocker, lire et administrer des données sur HDFS ; développer des jobs MapReduce en Python (Hadoop Streaming) avec le réflexe du test local avant le cluster ; requêter HDFS en SQL via Hive (tables externes, partitions, `EXPLAIN`) ; modéliser et manipuler des données HBase (shell, HappyBase) ; construire un flux d'ingestion NiFi → HDFS (processeurs, provenance, gestion des échecs) ; conduire un projet complet de bout en bout, du fichier brut au résultat servi et visualisé.

## Prérequis

Ligne de commande Linux (shell, droits, processus) ; SQL ; Python de base. Docker et Docker Compose installés sur la machine de l'apprenant (ou accès à la VM du cours). Niveau initié — aucune expérience Big Data n'est attendue.

## Le parcours

| # | Chapitre | Repère | Ce qu'on y fait |
|---|---|---|---|
| [01](01-introduction-hadoop.md) | Introduction à Hadoop | 3 h | architecture, HDFS/MapReduce/YARN en une vue, écosystème et statut 2026 |
| [02](02-presentation-env-hadoop.md) | L'environnement de TP | 2 h | le cluster Docker Compose, les scripts, l'accès distant — EX 02 |
| [03](03-hdfs.md) | HDFS | 5 h | blocs, réplication, NameNode/DataNode, commandes — EX 03 |
| [04](04-mapreduce-et-yarn.md) | MapReduce & YARN | 6 h | mapper/reducer en Python, test local, suivi des jobs — EX 04, TD 01 |
| [05](05-hive.md) | Hive | 4 h | tables externes, partitions, `EXPLAIN` — EX 05, TD 02 |
| [06](06-hbase-et-zookeeper.md) | HBase & ZooKeeper | 2 h | modèle clé/valeur, shell, HappyBase — EX 06, TD 03 |
| [07](07-nifi.md) | NiFi | 3 h | FlowFiles, processeurs, provenance, ingestion vers HDFS — EX 07, TD 04 |
| [TP-01](tp/TP-01-enonce.md) | TP final — analyse des ventes par ville | 1 jour | toute la chaîne sur une donnée, rapport et livrables — TD 05 en échauffement |
