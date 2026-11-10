![Diginamic Logo](../assets/diginamic.png) ![HBase Logo](../assets/hbase.jpeg)

# **Exercice 06 : HBase**

> **Type** : exercice guidé (parties I-II) + exercices autonomes (parties III-IV)
> **Prérequis** : séquence 06 (HBase & ZooKeeper), EX 04 (MapReduce)
> **Durée estimée** : 1 h 30 (parties I-II) + 1 h à 2 h (parties III-IV)
> **Lien cours** : [`../06 — HBase`](../06-hbase-et-zookeeper.md) — section [« HBase et Python : HappyBase »](../06-hbase-et-zookeeper.md#hbase-et-python--happybase)

## Table des matières

- [I. Première manipulation de HBase (shell)](#i-première-manipulation-de-hbase-shell)
- [II. HBase depuis Python : HappyBase](#ii-hbase-depuis-python--happybase)
- [III. Exercices autonomes](#iii-exercices-autonomes)
- [IV. Exercices MapReduce + HBase (dataset Spotify)](#iv-exercices-mapreduce--hbase-dataset-spotify)

---

## **I. Première manipulation de HBase (shell)**

---

De manière générale, vous aurez très peu de retour sur les commandes hbase shell. Il faudra tout le temps passer par un Get ou scan pour voir les données.

> **ATTENTION:** L'emplacement des **'** et des **"** sont importants, car on aura les 2 dans certaines commandes et que si on ne met pas les bons, la commande ne fonctionnera pas.

### **1. HBase Shell**

Démarrez HBase depuis le master : `./start-hbase.sh` (ZooKeeper doit être démarré avant — `./start-zookeeper.sh`).

> Le **ThriftServer** n'est pas nécessaire pour la partie I : `hbase shell` parle directement au cluster. Il ne le deviendra qu'à la partie II, avec HappyBase — lancez-le à ce moment-là par `./start-thrift.sh` et vérifiez avec `jps | grep Thrift`.

Pour manipuler votre bdd avec son **Shell** interactif, vous devez lancer le script suivant:
- `hbase shell`

Vous obtiendrez une interface ressemblant à la suivante:

![hbase shell](../assets/hbase-shell.png)

Vous pourrez fermer le terminal hbase en faisant `exit`.

---

### **2. Création de la table**

Pour créer une table dans HBase avec deux familles de colonnes différentes et spécifier un namespace :

```bash
# create 'table', 'cf1', 'cf2', ...
create 'store', 'customer', 'sales'
```

- la table 'store' est créée
- 'customer' et 'sales' sont les noms des familles de colonnes.

### 3. **Insertion de données (Create)**

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

---

### 4. **Mise à jour de données (Update)**

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

### 5. **Lecture de données (Read)**

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

---

### 6. **Suppression de données (Delete)**

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

---

### 7. **Lecture de données filtrées (Read)**

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

---

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

---

## II. HBase depuis Python : HappyBase

Le cours HappyBase (démarrage du service Thrift, syntaxes de connexion et de manipulation) est dans le support de cours : [**../06 — section « HBase et Python : HappyBase »**](../06-hbase-et-zookeeper.md#hbase-et-python--happybase).

**Préalable obligatoire** : sans ThriftServer, `happybase.Connection` échoue. Dans le master : `./start-thrift.sh`, puis vérifiez `jps | grep Thrift`. `happybase` **1.3.0 est déjà installé** dans l'image (aucun `pip install` à faire — vérifiez par `python3 -c "import happybase; print(happybase.__version__)"`).

Dans cet exercice, vous en aurez besoin pour les exercices III (HappyBase Python) et IV (MapReduce → HBase). Référez-vous au cours pour la connexion :

```python
import happybase
connection = happybase.Connection('127.0.0.1', 9090)  # depuis le conteneur
table = connection.table('ma_table')
```

> ⚠️ **N'appelez pas `connection.open()` après `happybase.Connection(...)`** : le paramètre `autoconnect` vaut `True` par défaut (vérifié sur happybase 1.3.0), la connexion est donc déjà ouverte et un `open()` explicite lève `TTransportException: Already open`. Écrivez `Connection(host, port, autoconnect=False)` seulement si vous voulez ouvrir plus tard.

> Depuis la machine hôte (hors conteneurs) : adresse de la machine Docker, port 9090 exposé tel quel.

---

## **III. Exercices autonomes**

---

### **hbase shell**

Quelles sont les commandes hbase pour faire ses instructions ?

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

### **Happybase Python**

1. Ecrire le code Python qui permet de réaliser les instructions précédentes depuis l'environnement hadoop.
2. Adapter le code pour contrôler le HBase distant depuis votre poste local.

**Résultat attendu** : le même contenu que via le shell (la table `bibliothèque` manipulée par API), preuve que Thrift expose exactement les mêmes données.

:pushpin: **AIDE:** Utilisez l'IP de la machine et le bon port public relié au port privé que vous souhaitez utiliser.

---

## IV. Exercices MapReduce + HBase (dataset Spotify)

Ces exercices venaient de l'[`04-mapreduce.md`](04-mapreduce.md) (exercices 6-7) — ils vivent désormais **ici**, dans la séquence HBase : insérer dans HBase les agrégats calculés par MapReduce, puis les relire en Python (HappyBase) pour produire un graphique.

### Exercice 4 : insertion dans HBase des streams par année

**Objectif** : Utiliser MapReduce pour insérer les résultats dans HBase.

- **Mapper** : extraire l'année de sortie (`released_year`) et le nombre de streams (`streams`).
- **Reducer** : agréger le nombre total de streams par année, puis insérer ces données agrégées dans une table HBase.

**Résultat attendu** : une table HBase `streams_year` contenant une ligne par année, avec column families pour les statistiques. Référence : [`../annexes/utils/5-reducer-hbase.py`](../annexes/utils/5-reducer-hbase.py). Vérifiez avec `scan 'streams_year'` dans le shell HBase.

### Exercice 5 : statistiques visuelles avec stockage HBase

**Objectif** : Combiner MapReduce, HappyBase et Matplotlib.

- **Mapper** : extraire le nom de l'artiste (`artist(s)_name`) et le nombre de streams (`streams`).
- **Reducer** : agréger le nombre total de streams par artiste, puis insérer les résultats dans HBase.
- **Autre fichier python** : récupérer les données depuis HBase et créer un graphique des streams par artiste, exporté en format PDF.

**Résultat attendu** : la table `streams_artist` remplie + un PDF de graphique streams par artiste généré par un script séparé qui lit HBase.

**Consignes d'alignement, communes aux deux :**

- **Nom des tables** : `streams_year` (exercice 4) et `streams_artist` (exercice 5).
- **Squelettes** : [`../annexes/utils/5-reducer-hbase.py`](../annexes/utils/5-reducer-hbase.py) (reducer qui écrit dans HBase) et [`../annexes/utils/6-reducer-matplotlib-pdf.py`](../annexes/utils/6-reducer-matplotlib-pdf.py) (graphique matplotlib, à utiliser dans le script séparé qui lit HBase).
- **Vérification** : le `put` d'HappyBase ne confirme rien — la preuve, c'est un `scan 'streams_year'` (ou `scan 'streams_artist'`) dans le shell HBase, pas le retour de la commande.
