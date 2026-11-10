[← Chapitre précédent](05-hive.md) · [Sommaire](Readme.md) · [Chapitre suivant →](07-nifi.md)

![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# 06 - HBase

> **Objectifs** : comprendre le modèle de données HBase et son architecture (régions, RegionServers, HMaster) · le rôle de ZooKeeper · manipuler HBase via le shell et HappyBase.
> **Mise en pratique** : [Exercices](#x-exercices)
## Sommaire

- [I. Introduction à HBase](#i-introduction-à-hbase)
  - [A. Pourquoi utiliser HBase ?](#a-pourquoi-utiliser-hbase-)
  - [B. Caractéristiques principales de HBase](#b-caractéristiques-principales-de-hbase)
  - [C. Principe de fonctionnement](#c-principe-de-fonctionnement)
  - [D. HBase, un modèle de données souple](#d-hbase-un-modèle-de-données-souple)
- [II. HBase et le NoSQL dans l’écosystème Hadoop](#ii-hbase-et-le-nosql-dans-lécosystème-hadoop)
  - [A. Limites du modèle Hadoop classique](#a-limites-du-modèle-hadoop-classique)
  - [B. Qu’est-ce que le NoSQL ?](#b-quest-ce-que-le-nosql-)
  - [C. Pourquoi utiliser des bases NoSQL ?](#c-pourquoi-utiliser-des-bases-nosql-)
- [III. HBase Modèle de Données](#iii-hbase-modèle-de-données)
- [IV. HBase Architecture](#iv-hbase-architecture)
- [V. HBase et ZooKeeper](#v-hbase-et-zookeeper)
  - [A. Rôle de ZooKeeper dans HBase](#a-rôle-de-zookeeper-dans-hbase)
  - [B. Le rôle du HBase Master](#b-le-rôle-du-hbase-master)
  - [C. Le rôle des RegionServers](#c-le-rôle-des-regionservers)
  - [D. Le concept de Région](#d-le-concept-de-région)
- [VI. HBase Stockage des tables](#vi-hbase-stockage-des-tables)
- [VII. HBase Namespaces](#vii-hbase-namespaces)
- [VIII. HBase et Python : HappyBase](#viii-hbase-et-python--happybase)
  - [A. Installer et démarrer](#a-installer-et-démarrer)
  - [B. Connexion et manipulation](#b-connexion-et-manipulation)
- [IX. HBase Liens web sur les commandes](#ix-hbase-liens-web-sur-les-commandes)
  - [A. Shell de HBase](#a-shell-de-hbase)
- [X. Exercices](#x-exercices)
  - [A. Première manipulation de HBase (shell)](#a-première-manipulation-de-hbase-shell)
  - [B. HBase depuis Python : HappyBase](#b-hbase-depuis-python--happybase)
  - [C. Exercices autonomes](#c-exercices-autonomes)
  - [D. Exercices MapReduce + HBase (dataset Spotify)](#d-exercices-mapreduce--hbase-dataset-spotify)
  - [Exercice 3 — insertion dans HBase des streams par année](#exercice-3--insertion-dans-hbase-des-streams-par-année)
  - [Exercice 4 — statistiques visuelles avec stockage HBase](#exercice-4--statistiques-visuelles-avec-stockage-hbase)
- [XI. Vérifiez vos acquis](#xi-vérifiez-vos-acquis)

## I. Introduction à HBase

### A. Pourquoi utiliser HBase ?

Les volumes de données actuels atteignent des échelles très élevées, allant du **téraoctet** au **pétaoctet**, voire au-delà. Si le **scaling out** (ajout de machines standards) permet de gérer cette croissance à moindre coût, certains cas d’usage dépassent les capacités d’Hadoop seul.

En particulier, Hadoop n’est pas adapté aux besoins nécessitant :
* des **écritures aléatoires fréquentes**,
* des **lectures aléatoires rapides**.

👉 Les bases de données relationnelles classiques ne sont donc plus suffisantes dans ces contextes.
**HBase répond à ces besoins en apportant un stockage distribué orienté accès temps quasi réel.**

### B. Caractéristiques principales de HBase

* une base de données **distribuée**, **orientée colonnes** et **multidimensionnelle** ;
* conçue pour la **haute disponibilité** et les **hautes performances** ;
* un **système de stockage** s’appuyant sur **HDFS**.

### C. Principe de fonctionnement

L’architecture HBase repose sur deux types de nœuds :
* **HBase Master** (actif à un instant donné) :
  * gère les opérations globales du cluster ;
  * répartit la charge, assigne les régions et supervise leur découpage ;
  * s’appuie sur **ZooKeeper** pour assurer la haute disponibilité.
* **RegionServers** :
  * hébergent les tables et leurs régions ;
  * exécutent les opérations de **lecture** et **écriture** ;
  * communiquent directement avec les clients.

### D. HBase, un modèle de données souple

* seules les **familles de colonnes** sont définies à l’avance ;
* les colonnes sont créées **dynamiquement lors de l’insertion** ;
* les valeurs non présentes ne sont **pas stockées** (pas de `NULL`) ;
* les données sont stockées sous forme de **`byte[]`** ;
* les lignes sont **triées par clé** et stockées séquentiellement ;
* les colonnes d’une même famille sont également **ordonnées**.

## II. HBase et le NoSQL dans l’écosystème Hadoop

### A. Limites du modèle Hadoop classique

Hadoop repose historiquement sur deux composants principaux :
* **HDFS** pour le stockage de grandes quantités de données ;
* **MapReduce** pour leur traitement distribué.

Ce modèle est particulièrement adapté au **traitement par lots (batch processing)**. Les données sont lues et écrites de manière **séquentielle**, sans interaction directe avec les utilisateurs.

Cependant, dans de nombreux cas :
* le traitement de fichiers volumineux produit à son tour d’autres fichiers volumineux ;
* ces fichiers doivent également être traités de façon séquentielle ;
* l’accès **aléatoire et rapide** aux données n’est pas possible.

👉 Une nouvelle approche est donc nécessaire pour stocker de très grands volumes de données tout en permettant des **lectures et écritures aléatoires efficaces**.

C’est dans ce contexte qu’apparaissent des solutions telles que **HBase, Cassandra, CouchDB, Dynamo ou MongoDB**, qui font partie des technologies de **bases de données NoSQL**.

### B. Qu’est-ce que le NoSQL ?

Le terme **NoSQL** (Not Only SQL) désigne un ensemble de technologies de bases de données conçues pour répondre aux contraintes du **Big Data** :
* données massives,
* données distribuées,
* structures hétérogènes et évolutives.

Contrairement aux bases relationnelles classiques, les bases NoSQL ne reposent pas sur un modèle relationnel strict et privilégient la **distribution**, la **scalabilité** et la **flexibilité**.

On distingue principalement **quatre grandes familles** de bases NoSQL :
* **Clé–valeur** : Redis, Memcached, Riak
* **Orientées graphe** : Neo4j, Sesame
* **Orientées colonnes** : HBase, Cassandra, SimpleDB
* **Orientées documents** : MongoDB, CouchDB

⚠️ Les technologies NoSQL **ne remplacent pas** les bases relationnelles ou les data warehouses, mais les **complètent** selon les cas d’usage.

### C. Pourquoi utiliser des bases NoSQL ?

Les bases NoSQL répondent à plusieurs besoins majeurs du Big Data :
* **Scalabilité horizontale** : ajout de machines plutôt qu’augmentation de la puissance d’un serveur ;
* **Coût maîtrisé** : utilisation de machines standards ;
* **Flexibilité** : modèle de données sans schéma strict ou à schéma évolutif ;
* **Haute disponibilité** : architectures distribuées et tolérantes aux pannes ;
* **Performances** : accès rapide à de grands volumes de données.

👉 **HBase** s’inscrit pleinement dans cette logique en apportant un stockage NoSQL distribué, intégré à Hadoop et capable de gérer des accès aléatoires à très grande échelle.

## III. HBase Modèle de Données

![hbase table model](assets/hbase-table-model.jpeg)

> **EXEMPLE : Comparaison HBase & RDBMS**

![hbase rdbms compare](assets/hbase-rdbms-compare.jpeg)

> **Plus spécifiquement, le modèle de données de HBase repose sur les concepts suivants :**

1. Les données sont stockées dans des tables HBase
2. Les tables contiennent des lignes et des colonnes
3. Chaque ligne est identifiée par une clé unique (RowKey)
4. Les colonnes sont regroupées dans des familles de colonnes (Column family)
5. Chacune des valeurs d'une cellule a une version désignée par un horodatage
6. Le schéma de table définit les familles de colonnes
    * On peut avoir un grand nombre variable de colonnes par ligne
    * (clé de ligne, clé de colonne, horodatage) -> valeur
    * Le tuple {ligne, colonne, version} spécifie exactement une cellule
7. Les lignes sont rangées dans l'ordre lexicographique (ordre binaire des octets) de clés de lignes
8. Il n'y a pas de schéma de colonnes figé dans HBase
    * On doit préciser les familles de colonnes possibles.
    * Elles déterminent l'organisation du stockage physique sur disque.
    * Chaque ligne peut avoir différents ensembles de familles de colonnes.
9. HBase est décrit comme un entrepôt de type Clé-Valeur
10. Chaque clé-valeur a un horodatage qui désigne une version:
    * Modifier une colonne consiste à ajouter une nouvelle version

## IV. HBase Architecture

- Vue Physique de cellules

![hbase row compo](assets/hbase-row-compo.jpeg)

- Architecture du Cluster

![hbase cluster](assets/hbase-cluster.jpeg)

## V. HBase et ZooKeeper

### A. Rôle de ZooKeeper dans HBase

**Apache ZooKeeper** fournit un **service de coordination distribué** indispensable au fonctionnement de HBase. Il joue un rôle central dans la **gestion de l’état du cluster** et dans la **tolérance aux pannes**.

ZooKeeper est notamment responsable de :

* la gestion des **métadonnées de correspondance entre régions et RegionServers** ;
* la détection de l’état des RegionServers via des **pulsations (heartbeats)** ;
* l’élection et la garantie de l’**unicité du HBase Master actif** ;
* l’aide à la **localisation des RegionServers** par les clients.

👉 Les clients utilisent ZooKeeper pour **localiser les RegionServers**, puis communiquent **directement** avec eux pour les opérations de lecture et d’écriture.

ZooKeeper est un composant **critique** : il doit toujours être **démarré avant HBase**.

```bash
./start-zookeeper.sh          # depuis /home du master
jps | grep Quorum             # doit afficher QuorumPeerMain
```

> Sur ce cluster il n'y a **aucun service SysVinit** : `service zookeeper-server status` et `sudo` n'ont rien à faire ici — on est déjà root dans le conteneur, et ZooKeeper est lancé par le script du cours ci-dessus. Le process à reconnaître dans `jps` est `QuorumPeerMain`.

### B. Le rôle du HBase Master

Le **HBase Master** est responsable de la **gestion globale du cluster**.
Un seul Master est actif à un instant donné ; les autres restent en **veille (backup)**.

Ses principales responsabilités sont :
* surveiller l’état de l’ensemble des **RegionServers** ;
* gérer les **opérations d’administration** :
  * création et suppression de tables,
  * gestion des familles de colonnes,
  * modifications de schéma ;
* assigner les **régions** aux RegionServers ;
* équilibrer la charge du cluster en redistribuant les régions entre les serveurs.

Lors de leur démarrage, les Masters se connectent à **ZooKeeper** :
* le premier devient **Master actif** ;
* les autres restent **Masters passifs**, prêts à prendre le relais.

### C. Le rôle des RegionServers

Les **RegionServers** sont les nœuds chargés du **stockage effectif des données** et de l’**exécution des lectures et écritures**.

Leur fonctionnement repose sur les concepts suivants :
* chaque RegionServer héberge **plusieurs régions** ;
* une **région** correspond à une portion d’une table ;
* chaque région contient plusieurs **Stores** ;
* chaque Store correspond à une **famille de colonnes**.

Un Store est composé de :
* un **MemStore** : tampon d’écriture en mémoire ;
* un ensemble de **HFiles** : fichiers persistants stockés sur HDFS.

Pour garantir la durabilité des données :
* chaque RegionServer possède un **WAL (Write-Ahead Log)** ;
* toute écriture est d’abord enregistrée dans le WAL avant d’être stockée définitivement dans les HFiles.

![hbase stockage schéma](assets/hbase-schema-stockage.png)

### D. Le concept de Région

Une **région** est une **partition horizontale d’une table HBase**, définie par :
* une clé de ligne de début,
* une clé de ligne de fin.

Caractéristiques principales :
* taille maximale : **1 Go sur ce cluster** — `hbase-site.xml` y pose `hbase.hregion.max.filesize = 1073741824`. Ne pas confondre avec les **128 Mo** de `dfs.blocksize`, qui sont la taille d'un *bloc HDFS* : une région est stockée sur HDFS sous forme de plusieurs blocs, les deux grandeurs sont indépendantes et se règlent séparément. C'est précisément le chaînon entre la séquence 03 et celle-ci ;
* unité fondamentale de **distribution** et de **disponibilité** des données ;
* une région est **automatiquement découpée** lorsqu’elle dépasse sa taille maximale ;
* un **équilibreur de charge** redistribue régulièrement les régions dans le cluster ;
* en cas de panne d’un RegionServer, ses régions sont **réassignées automatiquement** à d’autres serveurs.

👉 Les régions permettent à HBase de **passer à l’échelle** et de rester **hautement disponible**.

## VI. HBase Stockage des tables

![hbase shared explain](assets/hbase-shared-explain.jpeg)

## VII. HBase Namespaces

- Un namespace est un groupement logique de tables similaire à une BD ou un schéma dans SGBDR.
- Il y a des namespace prédéfinis dans HBase:
  - HBase: c'est un namespace système contenant les tables internes (meta, namespace) de HBase utilisées pour stocker les métadonnées.
  - default: les tables créées sans préciser leur namespace, sont créées par défaut dans le namespace default.
- Commandes:
  - Pour avoir la liste des namespaces: `list_namespace`
  - Pour créer un nouveau namespace: `create_namespace 'nom_namespace'`
  - Pour avoir la description d’un namespace: `describe_namespace 'nom_namespace'`

**On doit supprimer toutes les tables du namespace avant de le supprimer.**

- Pour supprimer un namespace :
  `drop_namespace 'nom_namespace'`
- Créer une table dans un namespace
  `create 'nom_namespace:nom_table', 'cf1', 'cf2', …`
- Lister les tables d’un namespace :
  `list_namespace_tables 'nom_namespace'`

## VIII. HBase et Python : HappyBase

HBase fournit une API en Python : [HappyBase](https://happybase.readthedocs.io/en/latest/). HappyBase est une bibliothèque conviviale permettant d'interagir avec Apache HBase ; sous la surface, elle utilise la bibliothèque Python Thrift pour se connecter à HBase via sa passerelle Thrift.

Dans le contexte de HBase, Java est le seul langage qui peut accéder directement à HBase. L'interface Thrift agit comme un pont qui permet à d'autres langages d'accéder à HBase : un serveur Thrift interagit avec le client Java et expose une API réseau.

![happybase](assets/happybase.png)

### A. Installer et démarrer

**Rien à installer** : `happybase` 1.3.0 est déjà présent dans l'image du cours (cf. séquence 02). Vérifiez plutôt :

```bash
python3 -c "import happybase; print(happybase.__version__)"   # 1.3.0
```

Un démon HBase **et son ThriftServer** doivent tourner pour que HappyBase puisse répondre — sans Thrift, la connexion échoue :

```bash
cd /home
./start-zookeeper.sh      # HBase exige ZooKeeper démarré avant lui
./start-hbase.sh
./start-thrift.sh
jps | grep Thrift         # doit afficher ThriftServer
```

> **Piège de `PATH`** : `/home` n'est **pas** dans le `PATH` du conteneur. Écrivez `./start-hbase.sh`, pas `start-hbase.sh` — sans le `./`, le shell trouve d'abord le script **d'HBase** (`/opt/hbase/bin/start-hbase.sh`), qui porte le même nom que celui du cours mais ne fait pas la même chose (vérifié par `command -v`). Le cas inverse existe aussi : `start-thrift.sh` n'existe **que** dans `/home`, donc sans `./` c'est `command not found`. Règle simple : **les scripts du cours se lancent toujours avec `./`**.

![start thrift](assets/start-thrift.png)

### B. Connexion et manipulation

Par défaut, le service thrift écoute sur le port **9090**. On se connecte depuis Python à la table déjà créée (ex. `store`, voir l'EX 06) :

```python
#!/usr/bin/env python

import happybase
connection = happybase.Connection('127.0.0.1', 9090)
connection.open()

table = connection.table('store')
print("La liste des tables sur HBase est : ")
print(connection.tables())
print(" nous avons aussi la table : ")
print(table)
connection.close()
```

![script 1](assets/script-happybase-1.png)

> Depuis la machine hôte (hors conteneur), remplacer `127.0.0.1` par l'adresse de la machine qui héberge Docker — le port 9090 reste exposé tel quel.

**Syntaxes HappyBase :**

1. Création des tables :

   ```python
   connection.create_table(
      'mytable',
      {
         'cf1': dict(max_versions=10),
         'cf2': dict(max_versions=1, block_cache_enabled=False),
         'cf3': dict(),
      }
   )
   ```

2. Connexion à une table :

   ```python
   table = connection.table('mytable')
   ```

3. Récupération des données :

   ```python
   row = table.row('row-key')
   print(row[b'cf1:col1'])

   rows = table.rows(['row-key-1', 'row-key-2'])
   for key, data in rows:
      print(key, data)

   for key, data in table.scan():
      print(key, data)
   ```

   Toutes les lignes à partir de la ligne `aaa` jusqu'à la fin de la table :

   ```python
   for key, data in table.scan(row_start='aaa'):
      print(key, data)

   for key, data in table.scan(row_stop='xyz'):
      print(key, data)

   for key, data in table.scan(row_start='aaa', row_stop='xyz'):
      print(key, data)
   ```

4. Stockage des données :

   ```python
   table.put(
      'row-key',
      {
         b'cf:col1': b'value1',
         b'cf:col2': b'value2'
      }
   )
   ```

   ```python
   #!/usr/bin/env python

   import happybase
   connection = happybase.Connection('127.0.0.1', 9090)
   connection.open()
   print(connection.tables())
   table = connection.table('store')
   rows = table.rows(['1001', '1002'])
   for key, data in rows:
      print(key, data)
   connection.close()
   ```

5. Résultat :

 ![script 2](assets/script-happybase-2.png)

## IX. HBase Liens web sur les commandes

### A. Shell de HBase

#### 1. General Commands

- `status` - Provides the status of HBase, for example, the number of servers.
- `version` - Provides the version of HBase being used.
- `table_help` - Provides help for table-reference commands.
- `whoami` - Provides information about the user.

#### 2. Data Definition Language

- `create` - Creates a table.
- `list` - Lists all the tables in HBase.
- `disable` - Disables a table.
- `is_disabled` - Verifies whether a table is disabled.
- `enable` - Enables a table.
- `is_enabled` - Verifies whether a table is enabled.
- `describe` - Provides the description of a table.
- `exists` - Verifies whether a table exists.
- `drop` - Drops a table from HBase.
- `drop_all` - Drops the tables matching the ‘regex’ given in the command.

#### 3. Data Manipulation Language

- `put` - Puts a cell value at a specified column in a specified row in a particular table.
- `get` - Fetches the contents of row or a cell.
- `delete` - Deletes a cell value in a table.
- `deleteall` - Deletes all the cells in a given row.
- `scan` - Scans and returns the table data.
- `count` - Counts and returns the number of rows in a table.

- **Tutorialspoint :**
  [HBase - Shell (tutorialspoint.com)](https://www.tutorialspoint.com/hbase/hbase_shell.htm)

- **Projet apache :**
  [Apache HBase – Apache HBase™ Home](https://hbase.apache.org/)
  [Apache HBase ™ Reference Guide](https://hbase.apache.org/book.html#_overriding_configuration_starting_the_hbase_shell)

## X. Exercices

### A. Première manipulation de HBase (shell)

De manière générale, vous aurez très peu de retour sur les commandes hbase shell. Il faudra tout le temps passer par un Get ou scan pour voir les données.

> **ATTENTION:** L'emplacement des **'** et des **"** sont importants, car on aura les 2 dans certaines commandes et que si on ne met pas les bons, la commande ne fonctionnera pas.

#### 1. HBase Shell

Démarrez HBase depuis le master : `./start-hbase.sh` (ZooKeeper doit être démarré avant — `./start-zookeeper.sh`).

> Le **ThriftServer** n'est pas nécessaire pour la partie A : `hbase shell` parle directement au cluster. Il ne le deviendra qu'à la partie B, avec HappyBase — lancez-le à ce moment-là par `./start-thrift.sh` et vérifiez avec `jps | grep Thrift`.

Pour manipuler votre bdd avec son **Shell** interactif, vous devez lancer le script suivant:
- `hbase shell`

Vous obtiendrez une interface ressemblant à la suivante:

![hbase shell](assets/hbase-shell.png)

Vous pourrez fermer le terminal hbase en faisant `exit`.

#### 2. Création de la table

Pour créer une table dans HBase avec deux familles de colonnes différentes et spécifier un namespace :

```bash
# create 'table', 'cf1', 'cf2', ...
create 'store', 'customer', 'sales'
```

- la table 'store' est créée
- 'customer' et 'sales' sont les noms des familles de colonnes.

#### 3. Insertion de données (Create)

Pour insérer des données dans HBase, utilisez la commande `put`.

```bash
# put 'table', 'rowkey', 'family:colonne', 'data'
put 'store', '1001', 'customer:name', 'John White'
put 'store', '1001', 'customer:city', 'Los Angeles, CA'
put 'store', '1001', 'sales:product', 'Chairs'
put 'store', '1001', 'sales:amount', '$400.00'

put 'store', '1002', 'customer:name', 'Jane Brown'
put 'store', '1002', 'customer:city', 'Antlanta, GA'
put 'store', '1002', 'sales:product', 'Lamps'
put 'store', '1002', 'sales:amount', '$200.00'

put 'store', '1003', 'customer:name', 'Bill Green'
put 'store', '1003', 'customer:city', 'Pittsburgh, PA'
put 'store', '1003', 'sales:product', 'Desk'
put 'store', '1003', 'sales:amount', '$500.00'

put 'store', '1004', 'customer:name', 'Jack Black'
put 'store', '1004', 'customer:city', 'St. Louis, MO'
put 'store', '1004', 'sales:product', 'Bed'
put 'store', '1004', 'sales:amount', '$1,600.00'
```

**Résultat attendu** : chaque `put` répond `0 row(s) in 0.0050 seconds` (HBase ne confirme pas, il faudra un `scan` pour vérifier — c'est normal).

#### 4. Mise à jour de données (Update)

Pour mettre à jour une valeur spécifique dans HBase, utilisez à nouveau la commande `put` — même commande que l'insertion : HBase versionne, la dernière valeur écrase.

```bash
# put 'table', 'row_key', 'cf:colonne', 'modification'
put 'store', '1002', 'sales:amount', '$850.00'

# ROW          COLUMN+CELL
#  1002        column=customer:city, timestamp=1720091566406, value=Antlanta, GA
#  1002        column=customer:name, timestamp=1720091566379, value=Jane Brown
#  1002        column=sales:amount, timestamp=1720091574415, value=$850.00
#  1002        column=sales:product, timestamp=1720091566425, value=Lamps
```

#### 5. Lecture de données (Read)

Pour lire une ligne précise, utilisez `get` avec son row_key.

```bash
# get 'table', 'row_key'
get 'store', '1001'

# ROW          COLUMN+CELL
#  1001        column=customer:city, timestamp=1720091566297, value=Los Angeles, CA
#  1001        column=customer:name, timestamp=1720091566267, value=John White
#  1001        column=sales:amount, timestamp=1720091566349, value=$400.00
#  1001        column=sales:product, timestamp=1720091566320, value=Chairs
```

Pour lire toutes les données, utilisez `scan`.

```bash
# scan 'table'
scan 'store'

# ROW          COLUMN+CELL
#  1001        column=customer:city, timestamp=1720091566297, value=Los Angeles, CA
#  ...
# 4 row(s) in 0.0240 seconds
```

#### 6. Suppression de données (Delete)

Pour supprimer une valeur spécifique, utilisez la commande `delete`.

```bash
# delete 'table', 'row_key', 'cf:colonne'
delete 'store', '1003', 'sales:amount'

# COLUMN                CELL
#  customer:city        timestamp=1720091566495, value=Pittsburgh, PA
#  customer:name        timestamp=1720091566478, value=Bill Green
#  sales:product        timestamp=1720091566509, value=Desk
```

Pour supprimer entièrement une ligne, utilisez la commande `deleteall`.

```bash
deleteall 'store', '1004'

# COLUMN                                                       CELL
# 0 row(s) in 0.2140 seconds
```

#### 7. Lecture de données filtrées (Read)

Afin d'avoir uniquement ce que l'on souhaite, il existe plusieurs filtres comme LIMIT, COLUMNS, FILTER...

La structure de la commande : `scan 'table', {OPTION1 => "filtre"}`

> Utilisation de LIMIT

```bash
# Retourne les deux premières lignes de la table `store`.
scan 'store', { LIMIT => 2 }

# ROW             COLUMN+CELL
#  1001           column=customer:city, timestamp=1720091566297, value=Los Angeles, CA
#  ...
# 2 row(s) in 0.0120 seconds
```

> Utilisation de COLUMNS

```bash
# Retourne les valeurs des colonnes `customer:name` et `sales:amount` pour toutes les lignes.
scan 'store', { COLUMNS => ['customer:name', 'sales:amount'] }

# ROW             COLUMN+CELL
#  1001           column=customer:name, timestamp=1720091566267, value=John White
#  ...
# 3 row(s) in 0.0100 seconds
```

> Utilisation de FILTER

Pour appliquer un filtre aux résultats du scan, utilisez l'option `FILTER`. Il existe plusieurs types de filtres, mais nous utiliserons ici `SingleColumnValueFilter`.

```bash
# Retourne les lignes où la colonne `sales:amount` a une valeur de 400.
scan 'store', { FILTER => "SingleColumnValueFilter('sales', 'amount', =, 'binary:$400.00')" }

# ROW             COLUMN+CELL
#  1001           column=customer:city, timestamp=1720091566297, value=Los Angeles, CA
#  ...
# 2 row(s) in 0.0160 seconds
```

:warning: les colonnes qui n'ont pas de données seront affichées dans ce cas.

> Combinaison d'options

Le but final est de pouvoir les combiner : on enchaine les options et les filtres dans les accolades : `scan 'table', {OPTION1 => "filtre", OPTION2 => "filtre"}`

```bash
# affiche uniquement le nom du client et le produit pour tous les clients qui ont acheté un objet à 400$
scan 'store', { COLUMNS => ['customer:name', 'sales:amount'], FILTER => "SingleColumnValueFilter('sales', 'amount', =, 'binary:$400.00')" }

# ROW                                                          COLUMN+CELL
#  1001                                                        column=customer:name, timestamp=1720091566267, value=John White
#  ...
# 2 row(s) in 0.0170 seconds
```

### B. HBase depuis Python : HappyBase

Le cours HappyBase (démarrage du service Thrift, syntaxes de connexion et de manipulation) est dans ce chapitre : [« HBase et Python : HappyBase »](#viii-hbase-et-python--happybase).

**Préalable obligatoire** : sans ThriftServer, `happybase.Connection` échoue. Dans le master : `./start-thrift.sh`, puis vérifiez `jps | grep Thrift`. `happybase` **1.3.0 est déjà installé** dans l'image (aucun `pip install` à faire — vérifiez par `python3 -c "import happybase; print(happybase.__version__)"`).

Dans cet exercice, vous en aurez besoin pour les exercices de la partie C (HappyBase Python) et de la partie D (MapReduce → HBase). Référez-vous au cours pour la connexion :

```python
import happybase
connection = happybase.Connection('127.0.0.1', 9090)  # depuis le conteneur
table = connection.table('ma_table')
```

> ⚠️ **N'appelez pas `connection.open()` après `happybase.Connection(...)`** : le paramètre `autoconnect` vaut `True` par défaut (vérifié sur happybase 1.3.0), la connexion est donc déjà ouverte et un `open()` explicite lève `TTransportException: Already open`. Écrivez `Connection(host, port, autoconnect=False)` seulement si vous voulez ouvrir plus tard.

> Depuis la machine hôte (hors conteneurs) : adresse de la machine Docker, port 9090 exposé tel quel.

### C. Exercices autonomes

#### 1. hbase shell

Quelles sont les commandes hbase pour faire ces instructions ?

1. Créer la table **bibliothèque** avec deux colonnes **auteur** (nom et prénom), **livre** (titre, categ, date).
2. Ajouter les données suivantes :

   | row_key | auteur:nom   | auteur:prenom | livre:titre| livre:categ  | livre:date |
   | ------- | ------------ | ------ | ---------------------- | ------ | ---- |
   | vhugo   | Hugo         | Victor | La légende des siècles | Poèmes | 1855 |
   | jverne  | Verne        | Jules  | Face au drapeau        | Roman  | 1896 |

3. Afficher toutes les données de la table.
4. Afficher la liste des livres stockés pour vhugo.
5. Modifier la date du livre de 'vhugo' pour 2024.
6. Supprimer 'livre:categ' de 'vhugo'.
7. Afficher les livres qui sont sortis après la fin du 20e siècle.
8. Supprimez toutes les données de 'jverne'

**Résultat attendu (Q3)** :

```
ROW    COLUMN+CELL
 vhugo column=auteur:nom, timestamp=..., value=Hugo
 vhugo column=auteur:prenom, timestamp=..., value=Victor
 vhugo column=livre:categ, timestamp=..., value=Poèmes
 vhugo column=livre:date, timestamp=..., value=1855
 vhugo column=livre:titre, timestamp=..., value=La légende des siècles
 jverne column=auteur:nom, ...
 2 row(s)
```

> **Indice (Q7)** : le filtre `SingleColumnValueFilter` accepte d'autres opérateurs que `=` — essayez `>=` avec `binary:2000`. Les valeurs sont comparées en binaire : les dates de 4 chiffres se comparent correctement de gauche à droite (1855 < 2024).

<details>
<summary>Voir la correction</summary>

**Q1-Q2. Création et insertion**

```bash
create 'bibliotheque', 'auteur', 'livre'

put 'bibliotheque', 'vhugo', 'auteur:nom', 'Hugo'
put 'bibliotheque', 'vhugo', 'auteur:prenom', 'Victor'
put 'bibliotheque', 'vhugo', 'livre:titre', 'La légende des siècles'
put 'bibliotheque', 'vhugo', 'livre:categ', 'Poèmes'
put 'bibliotheque', 'vhugo', 'livre:date', '1855'

put 'bibliotheque', 'jverne', 'auteur:nom', 'Verne'
put 'bibliotheque', 'jverne', 'auteur:prenom', 'Jules'
put 'bibliotheque', 'jverne', 'livre:titre', 'Face au drapeau'
put 'bibliotheque', 'jverne', 'livre:categ', 'Roman'
put 'bibliotheque', 'jverne', 'livre:date', '1896'
```

**Q3. Tout afficher**

```bash
scan 'bibliotheque'
```

**Q4. Livres de vhugo**

```bash
get 'bibliotheque', 'vhugo'
# ou : scan 'bibliotheque', { LIMIT => 1 }  (vhugo est la première row key)
```

**Q5. Modifier la date**

```bash
put 'bibliotheque', 'vhugo', 'livre:date', '2024'
```

**Q6. Supprimer la catégorie**

```bash
delete 'bibliotheque', 'vhugo', 'livre:categ'
```

**Q7. Livres sortis après la fin du 20e siècle**

```bash
scan 'bibliotheque', { FILTER => "SingleColumnValueFilter('livre', 'date', >=, 'binary:2000')" }
```

> Le comparateur `binary:` compare les dates comme des chaînes binaires : '2024' ≥ '2000' fonctionne car les 4 caractères se comparent de gauche à droite.

**Q8. Supprimer jverne**

```bash
deleteall 'bibliotheque', 'jverne'
```

</details>

#### 2. Happybase Python

1. Ecrire le code Python qui permet de réaliser les instructions précédentes depuis l'environnement hadoop.
2. Adapter le code pour contrôler le HBase distant depuis votre poste local.

**Résultat attendu** : le même contenu que via le shell (la table `bibliothèque` manipulée par API), preuve que Thrift expose exactement les mêmes données.

:pushpin: **AIDE:** Utilisez l'IP de la machine et le bon port public relié au port privé que vous souhaitez utiliser.

<details>
<summary>Voir la correction</summary>

```python
#!/usr/bin/env python
import happybase

connection = happybase.Connection('127.0.0.1', 9090)
# pas de connection.open() : autoconnect vaut True par defaut (happybase 1.3.0),
# un open() explicite leve TTransportException: Already open

# Création (si la table shell existe déjà, la supprimer d'abord ou renommer)
try:
    connection.disable_table('bibliotheque_py')
    connection.delete_table('bibliotheque_py')
except Exception:
    pass
connection.create_table('bibliotheque_py', {'auteur': dict(), 'livre': dict()})
table = connection.table('bibliotheque_py')

# Q1-Q2 : création + insertion
table.put('vhugo', {b'auteur:nom': b'Hugo', b'auteur:prenom': b'Victor',
                    b'livre:titre': b'La légende des siècles',
                    b'livre:categ': b'Poèmes', b'livre:date': b'1855'})
table.put('jverne', {b'auteur:nom': b'Verne', b'auteur:prenom': b'Jules',
                     b'livre:titre': b'Face au drapeau',
                     b'livre:categ': b'Roman', b'livre:date': b'1896'})

# Q3 : tout afficher
for key, data in table.scan():
    print(key, data)

# Q4 : ligne de vhugo
print(table.row(b'vhugo'))

# Q5 : modification
table.put(b'vhugo', {b'livre:date': b'2024'})

# Q6 : suppression d'une colonne
table.delete(b'vhugo', [b'livre:categ'])

# Q8 : suppression d'une ligne
table.delete(b'jverne')

connection.close()
```

**Q2 (depuis la machine hôte)** : remplacer `('127.0.0.1', 9090)` par `('<adresse_machine_docker>', 9090)` — le port est exposé tel quel par le compose.

</details>

### D. Exercices MapReduce + HBase (dataset Spotify)

Ces exercices venaient de l'[`exercices/04-mapreduce.md`](exercices/04-mapreduce.md) (exercices 6-7) — ils vivent désormais **ici**, dans la séquence HBase : insérer dans HBase les agrégats calculés par MapReduce, puis les relire en Python (HappyBase) pour produire un graphique.

**Consignes d'alignement, communes aux deux :**

- **Nom des tables** : `streams_year` (exercice 3) et `streams_artist` (exercice 4).
- **Squelettes** : [`annexes/utils/5-reducer-hbase.py`](annexes/utils/5-reducer-hbase.py) (reducer qui écrit dans HBase) et [`annexes/utils/6-reducer-matplotlib-pdf.py`](annexes/utils/6-reducer-matplotlib-pdf.py) (graphique matplotlib, à utiliser dans le script séparé qui lit HBase).
- **Vérification** : le `put` d'HappyBase ne confirme rien — la preuve, c'est un `scan 'streams_year'` (ou `scan 'streams_artist'`) dans le shell HBase, pas le retour de la commande.

### Exercice 3 — insertion dans HBase des streams par année

**Objectif** : Utiliser MapReduce pour insérer les résultats dans HBase.

- **Mapper** : extraire l'année de sortie (`released_year`) et le nombre de streams (`streams`).
- **Reducer** : agréger le nombre total de streams par année, puis insérer ces données agrégées dans une table HBase.

**Résultat attendu** : une table HBase `streams_year` contenant une ligne par année, avec column families pour les statistiques. Référence : [`annexes/utils/5-reducer-hbase.py`](annexes/utils/5-reducer-hbase.py). Vérifiez avec `scan 'streams_year'` dans le shell HBase.

<details>
<summary>Voir la correction</summary>

**Exercice 3 — insertion HBase des streams par année.** Mapper identique au corrigé Spotify 1 ([`exercices/corriges/04-mapreduce.md`](exercices/corriges/04-mapreduce.md)). Reducer : cf. [`annexes/utils/5-reducer-hbase.py`](annexes/utils/5-reducer-hbase.py) — le squelette crée `streams_year` (ou `streams_artist`) et insère chaque agrégat avec `table.put()`. La row key est la clé du stdin (année ou artiste), pas un index. Pour la variante « par année », utiliser `streams_year` comme nom de table et l'année comme row key :

```python
# (connexion happybase : cf. cours 06 § HappyBase)
hbase_table = 'streams_year'
connection.create_table(hbase_table, {'info': dict(), 'stat': dict()})
table = connection.table(hbase_table)

current_year, total = None, 0
for line in sys.stdin:
    year, streams = line.strip().split('\t', 1)
    try:
        streams = int(streams)
    except ValueError:
        continue
    if current_year == year:
        total += streams
    else:
        if current_year:
            table.put(current_year, {b'stat:total_streams': str(total).encode()})
        current_year, total = year, streams
if current_year:
    table.put(current_year, {b'stat:total_streams': str(total).encode()})
```

Vérification shell : `scan 'streams_year'` liste chaque année avec son total.

</details>

### Exercice 4 — statistiques visuelles avec stockage HBase

**Objectif** : Combiner MapReduce, HappyBase et Matplotlib.

- **Mapper** : extraire le nom de l'artiste (`artist(s)_name`) et le nombre de streams (`streams`).
- **Reducer** : agréger le nombre total de streams par artiste, puis insérer les résultats dans HBase.
- **Autre fichier python** : récupérer les données depuis HBase et créer un graphique des streams par artiste, exporté en format PDF.

**Résultat attendu** : la table `streams_artist` remplie + un PDF de graphique streams par artiste généré par un script séparé qui lit HBase.

<details>
<summary>Voir la correction</summary>

**Exercice 4 — streams par artiste + PDF.** Structure : mapper (artiste, streams) → reducer agrégé qui insère en HBase (même squelette que ci-dessus, row key = artiste) → **script séparé** qui relit HBase et exporte le graphique :

```python
#!/usr/bin/env python
import happybase
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

connection = happybase.Connection('127.0.0.1', 9090)
# pas de connection.open() : autoconnect vaut True par defaut (happybase 1.3.0),
# un open() explicite leve TTransportException: Already open
table = connection.table('streams_artist')

artists, totals = [], []
for key, data in table.scan():
    artists.append(key.decode())
    totals.append(int(data[b'stat:total_streams']))

top = sorted(zip(artists, totals), key=lambda x: -x[1])[:10]
plt.figure(figsize=(10, 6))
plt.bar([a for a, _ in top], [s for _, s in top])
plt.xticks(rotation=45, ha='right')
plt.title('Top 10 des artistes par streams')
plt.tight_layout()
plt.savefig('top_artistes.pdf')
connection.close()
```

**Livrables** : table `streams_artist` remplie + fichier `top_artistes.pdf` généré par le script séparé.

</details>

## XI. Vérifiez vos acquis

*Formatif, jamais noté. Répondez sans relire le chapitre. Les réponses se déplient juste après le séparateur : ce qui compte est de repérer où vous vacillez, pas de faire 5 sur 5.*

**1.** Quel est le rôle de ZooKeeper vis-à-vis de HBase ?

- A. Il exécute les lectures et écritures à la place des RegionServers, ce qui évite au client d'avoir à se connecter directement à eux
- B. Il conserve les HFiles et les MemStores de chaque Store, et décide lui-même du moment où une région doit être découpée
- C. Il tient les métadonnées de correspondance entre régions et RegionServers et garantit l'unicité du Master actif
- D. Il remplace le HMaster lorsqu'il s'arrête, puis redevient passif dès que celui-ci redémarre

**2.** `hbase.hregion.max.filesize` vaut 1073741824 et `dfs.blocksize` 128 Mo sur ce cluster. HDFS (le système de fichiers distribué de Hadoop) stocke les régions : comment lire ces deux grandeurs ?

- A. Elles sont indépendantes et se règlent séparément : une région occupe plusieurs blocs HDFS
- B. La région doit rester sous la taille du bloc : au-delà de 128 Mo, HBase refuse de l'écrire sur HDFS
- C. Il faut monter `dfs.blocksize` à 1 Go pour qu'une région tienne dans un seul bloc HDFS
- D. Ce sont deux noms d'une même grandeur : `dfs.blocksize` côté HDFS, la taille de région côté HBase

**3.** Pour changer le montant de la ligne `1002`, l'exercice utilise à nouveau `put` et non une commande de mise à jour. Pourquoi ?

- A. Il faut d'abord `delete` l'ancienne valeur puis `put` la nouvelle : HBase n'accepte jamais deux versions d'une même cellule
- B. Modifier une colonne ajoute une nouvelle version horodatée, et c'est la plus récente que `get` renvoie à la lecture
- C. `put` ne modifie que les colonnes d'une famille déclarée au préalable, ce qui impose de repasser par le shell
- D. La mise à jour passe par `table.put()` en HappyBase, le shell HBase ne connaissant que création et suppression

**4.** Un script appelle `happybase.Connection('127.0.0.1', 9090)` puis `connection.open()`, ce qui lève `TTransportException: Already open`. Qu'est-ce qui explique cette erreur ?

- A. Le ThriftServer n'est pas démarré : il faudrait lancer `./start-thrift.sh` avant de se connecter
- B. Le port 9090 est celui du HMaster : il faut indiquer à la place le port du RegionServer qui sert la table
- C. `connection.open()` ne s'appelle qu'après `connection.table()` : l'ordre des deux instructions compte ici
- D. `autoconnect` vaut `True` par défaut sur happybase 1.3.0 : la connexion est donc déjà ouverte à ce stade

**5.** *Question ouverte (2-3 lignes).* Un RegionServer qui hébergeait des régions de la table `store` s'arrête brutalement. Qui reprend ces régions, comment un client retrouve-t-il le serveur qui les sert désormais, et qu'est-ce qui fait que les dernières écritures ne sont pas perdues ?

---

<details>
<summary>Vérifier ses réponses</summary>

### Réponses

**1. C.** ZooKeeper tient les métadonnées de correspondance régions / RegionServers, détecte l'état des serveurs par pulsations et garantit l'unicité du Master actif.
**Pourquoi :** le chapitre sépare les rôles — le client passe par ZooKeeper pour localiser, puis communique directement avec les RegionServers pour lire et écrire. Les HFiles et les MemStores appartiennent aux Stores, pas à ZooKeeper.

**2. A.** Les deux grandeurs sont indépendantes : une région est stockée sur HDFS sous forme de plusieurs blocs, et chacune se règle de son côté.
**Pourquoi :** `hbase.hregion.max.filesize` déclenche le découpage d'une région qui dépasse 1 Go, `dfs.blocksize` décide de la taille d'un bloc HDFS — les confondre, c'est confondre la partition logique de la table et le découpage physique du fichier.

**3. B.** Il n'y a pas de mise à jour en place : `put` ajoute une version horodatée, et la lecture renvoie la plus récente.
**Pourquoi :** le modèle de données précise que chaque valeur de cellule porte un horodatage et que « modifier une colonne consiste à ajouter une nouvelle version » ; la partie A de cette section réécrit `sales:amount` de `1002` avec `put` pour cette raison.

**4. D.** `autoconnect` vaut `True` par défaut sur happybase 1.3.0 : `happybase.Connection(...)` ouvre déjà la connexion, d'où `Already open`.
**Pourquoi :** le chapitre et l'exercice donnent la même consigne — n'appeler `open()` que si l'on a explicitement écrit `Connection(host, port, autoconnect=False)` pour ouvrir plus tard.

**5.** Attendu : les régions du serveur défunt sont réassignées automatiquement à d'autres RegionServers par le HMaster ; le client consulte ZooKeeper pour les localiser à nouveau, puis leur parle en direct ; et les écritures survivent parce que chaque RegionServer journalise d'abord dans son WAL (Write-Ahead Log) avant la persistance dans les HFiles.
**Pourquoi :** la réassignation automatique des régions et le WAL sont les deux mécanismes qui font d'HBase une base hautement disponible et durable, une fois ZooKeeper démarré avant HBase.

</details>

---

[← Chapitre précédent](05-hive.md) · [Sommaire](Readme.md) · [Chapitre suivant →](07-nifi.md)
