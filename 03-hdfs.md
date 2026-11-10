[← Chapitre précédent](02-presentation-env-hadoop.md) · [Sommaire](Readme.md) · [Chapitre suivant →](04-mapreduce-et-yarn.md)

![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# **HDFS (Hadoop File System)**

> **Objectifs** : comprendre l'architecture HDFS (NameNode, DataNode, blocs, réplication) · maîtriser les opérations de base (put, get, ls…) · configurer et démarrer le cluster.
> **Prérequis** : séquences 01-02 · **Durée** : ~5 h · **Type** : cours + manipulations
> **Mise en pratique** : [exercices/03 — HDFS](exercices/03-hdfs.md)

## **Sommaire**

- [Introduction HDFS](#introduction-hdfs)
  - [Pourquoi HDFS ?](#pourquoi-hdfs-)
  - [Présentation de HDFS](#présentation-de-hdfs)
  - [Accès à HDFS](#accès-à-hdfs)
  - [Comment utiliser HDFS](#comment-utiliser-hdfs)
  - [API HDFS](#api-hdfs)
- [Principes HDFS](#principes-hdfs)
  - [Caractéristiques générales](#caractéristiques-générales)
  - [Réplication](#réplication)
  - [Sous-réplication](#sous-réplication)
  - [Taille des blocs](#taille-des-blocs)
  - [Codecs de compression](#codecs-de-compression)
  - [Permissions](#permissions)
- [Architecture des services](#architecture-des-services)
  - [Fonctionnement](#fonctionnement)
  - [Le NameNode](#le-namenode)
  - [La communication](#la-communication)
  - [NameNode (rôle résumé)](#namenode-rôle-résumé)
  - [DataNode](#datanode)
  - [Secondary NameNode](#secondary-namenode)
- [Opérations HDFS](#opérations-hdfs)
- [Configurer et démarrer HDFS](#configurer-et-démarrer-hdfs)
  - [Initialiser et démarrer](#initialiser-et-démarrer)
  - [Démarrer le cluster HDFS](#démarrer-le-cluster-hdfs)

---

## **Introduction HDFS**

---

### **Pourquoi HDFS ?**

- Ce système de fichiers est distribué sur des centaines voire des milliers de serveurs, et chaque nœud stocke une partie du système fichier. Pour éviter le risque de perdre des données, chaque donnée est stockée à plusieurs emplacements (ratio 3 par défaut — 1 sur le cluster du cours, voir plus bas la section Réplication).
- Il est très adapté au Big Data. Des dizaines de millions de fichiers peuvent être supportés sur une seule instance. Par ailleurs, cela assure la cohérence et l’intégrité des données pour éviter tout désagrément.
- HDFS est connu par sa portabilité. Il peut fonctionner sur différents types de commodity hardwares sans aucun problème de compatibilité.
- Plus qu’une base de données, ce système de fichier distribué se présente comme une Data Warehouse. Il est impossible de déployer un langage de requête *direct* sur HDFS : les données sont lues par des programmes MapReduce, ou requêtées en SQL via une couche dédiée (**Hive**, séquence 05) qui compile le SQL en MapReduce. Les données adhèrent à un modèle de cohérence simple et robuste.

### **Présentation de HDFS**

- HDFS est le système de fichiers de Hadoop.
- Comme tout système de fichiers, HDFS a vocation à être accédé à la fois :
  - pour écrire,
  - pour interroger,
  - pour lire les données qui y sont stockées.
- Cependant, on ne va jamais l'interroger en direct.
- En effet, Hadoop possède une couche d'abstraction nommée HadoopFS qui permet d'interroger, de lire et d'écrire des données directement dans un système de fichiers.
- Cette couche d'abstraction a le gros avantage d'avoir plusieurs backend et plusieurs frontend.
- En backend, on entend les systèmes de fichiers supportés.

### **Accès à HDFS**

- Ainsi, HadoopFS va permettre d'accéder à la fois à l'HDFS, mais également aux fichiers locaux,
- Et par exemple : à Amazon S3, aux fichiers stockés sur un FTP, à OpenStack Swift, à OrangeFS, Windows Azure, Cassandra, CephFS, le Google Storage, MapR-FS ou Quantcast ….
- Tous ces backend vont être gérés avec les mêmes interfaces, et ainsi un logiciel qui va utiliser HadoopFS, va pouvoir accéder aux données quel que soit le système de fichiers sur lequel elles sont stockées.
- Et ainsi, un outil qui utilise HadoopFS, va pouvoir accéder aux données quel que soit le système de fichiers utilisé.

### **Comment utiliser HDFS**

- Pour ce faire, on va utiliser des URI — des « Uniform Resource Identifier » — et plus précisément des URL, des « Uniform Resource Locator », pour savoir où les données sont stockées.
- A chaque fois, on va avoir donc, un protocole qui va définir le type de système de fichiers
- Ainsi, si on veut parler de l'HDFS, on va avoir notre URI qui va commencer par hdfs://, tout comme si on veut parler d'une ressource sur le système de fichiers local, on va utiliser file://.
- **Je vous encourage vivement à toujours utiliser HadoopFS sans spécifier le protocole afin de laisser la possibilité à l'utilisateur de votre application de le faire.**
- **Cela permettra que tous vous outils fonctionnent maintenant avec HadoopFS, puissent fonctionner quel que soit le système de fichiers derrière.**

### **API HDFS**

- HadoopFS met donc à disposition diverses API
- C'est-à-dire qu'on va pouvoir y accéder par exemple en ligne de commande, pour pouvoir interroger le système de fichiers quel qu'il soit ou via une API Java (ou aussi une API dans notre cas en Python avec une API JAVA)
- Qui permet de créer d'écrire, de modifier, de supprimer des fichiers.
- Via une librairie C, qui est en fait construite grâce à JNI, Java Native Interface, qui permet d'avoir les méthodes Java accessibles directement en C. Via Thrift, un protocole directement pour accéder aux données, qui est propre à Apache.
- Ou encore via Fuse par exemple, pour le monter dans l'espace utilisateur, ou en WebDAV, pour l'interroger, par exemple, grâce à une API HTTP.

---

## **Principes HDFS**

---

### **Caractéristiques générales**

- Concernant la Réplication : La Réplication est le fait d'avoir les données accessibles à plusieurs endroits simultanément.
- Cette Réplication, sur l'HDFS, se fait par blocs.
- C'est à dire que ce sont les blocs, non pas les fichiers, qui sont accessibles à plusieurs endroits simultanément.
- Elles ne concernent bien entendu que les fichiers.

### **Réplication**

- En effet, les dossiers, à proprement parler, n'existent que dans l'index.
- Par défaut, l'HDFS a un ratio de Réplication de 3.
- C'est à dire que chaque bloc de chaque fichier doit être à trois endroits simultanément. ⚠️ **Mais ce n'est qu'une valeur par défaut, modifiable** : sur le cluster du cours `dfs.replication=1` (configuré dans `hdfs-site.xml`, voir la séquence 01) — l'économie de place est au prix de la redondance.
- Il peut être personnalisable par fichier, ou par répertoire, grâce à la commande HDFS : **dfs setrep**.
- Il peut ainsi donc être ajusté en fonction des besoins, notamment si la donnée doit être plus répliquée
- Par exemple parce qu'elle est sensible, ou pour augmenter les performances en lecture, ou moins répliquée, parce qu'elle consomme beaucoup trop d'espace de stockage pour sa criticité.

### **Sous-réplication**

- On peut avoir des cas de sous-réplication, c'est à dire qu'à un moment donné, il n'existe pas assez de réplicas d'un même bloc
- Dans ce cas-là, le système essaye de le corriger au plus vite, et des cas de sur réplication
- C'est à dire qu'à un moment donné, trop de réplicas existent pour un même bloc, auquel cas, le système HDFS le corrige également au plus vite, simplement en supprimant un ou plusieurs réplicas.
- Le nombre de réplicas est visible dans chacune des interfaces, que ce soit l'interface web ou la ligne de commande. On a également la notion de Block Size.

### **Taille des blocs**

- Le Block Size étant la taille d'un bloc.
- Toutes les données étant stockées par bloc, l'espace de stockage étant découpé par blocs, ce paramètre se configure dans `hdfs-site.xml` et **peut être dérogé fichier par fichier** (par ex. `hdfs dfs -D dfs.blocksize=67108864 -put …`).
- ⚠️ **Contrevérité classique (y compris dans ce support) :** le block size *par défaut* du cluster (128 Mio) s'applique à tous les fichiers **sauf à ceux pour lesquels on a donné une valeur à la création** — il n'est donc pas « forcément le même pour tous les fichiers ».
- Plus le Block Size est gros, plus cela est adapté pour les gros fichiers et les fichiers peu nombreux.
- Plus il est faible, plus c'est adapté pour les petits fichiers et les fichiers très nombreux, et plus cela limite le surcoût, à proprement parler, de travailler par blocs.
- L'HDFS supporte également nativement la Compression. Ça veut dire que tout ce qui est écrit ou lu de l'HDFS peut être compressé ou décompressé à la volée.

### **Codecs de compression**

- On va passer par des codecs de compression, soit écrits en Java, soit natifs directement sur le système s'ils sont disponibles.
- Bien entendu, compresser réduit l'espace de stockage nécessaire pour stocker les données et augmente de ce fait les vitesses de transfert, également, puisque moins de données à transférer.
- Par contre, cela induit une charge CPU supplémentaire, et en plus, empêche les données de pouvoir être « splittées », c’est-à-dire découpées en plusieurs parties.
- Différents codecs de compression sont disponibles, on va retrouver notamment Gzip, Bzip2, Snappy et LZO.
- **Chaque codec ayant ses avantages et ses inconvénients en fonction de nos besoins.**

### **Permissions**

- l'HDFS a également un système de permission.
- De façon similaire à ce qu'on retrouve sous Linux, chaque fichier et répertoire a un propriétaire et un groupe.
- Les trois sont alors définis pour le propriétaire, le groupe et les autres, et à chaque fois, pour la lecture, l'écriture et la traversée du répertoire.
- On a également un sticky bit pour les répertoires et l'HDFS supporte les ACL POSIX pour avoir des permissions plus fines avec par exemple getfacl.
- Le système de permission peut se désactiver pour l'intégralité de l'HDFS mais dans ce cas-là, il peut cependant être nécessaire, toujours, d'utiliser le SuperUser qui est le user HDFS par défaut. Ce système de permission va de paire avec un système d'Authentification qui se fait soit de façon simple avec un login mot de passe, soit via Kerberos.

---

## **Architecture des services**

---

### **Fonctionnement**

- L'HDFS fonctionne grâce à deux types de service, le NameNode et un ou plusieurs DataNode.
- Les DataNodes vont être là où vont être stockés les données.
- Il peut donc s'agir de serveurs totalement différents avec de le hardware totalement différent avec un ou plusieurs disques, il faut juste éviter d'avoir des raids sur les DataNodes.
- Les DataNodes vont stocker les données, données qui sont répliquées, c'est-à-dire que la même donnée, par exemple A va se retrouver à plusieurs endroits et qui sont stockées par blocs. A, B, C, D, E, F et G sont, de ce fait-là, des blocs.

### **Le NameNode**

- Le NameNode quant à lui va avoir l'index des fichiers
- Il va savoir qu'il y a un fichier /totaux.txt qui va être composé des blocs A, B, C et D.
- Nous n'avons pas de blocs partiels, juste les blocs peuvent être vides sur leur fin, si jamais le fichier ne le remplit pas entièrement.
- C'est le NameNode qui va également avoir la structure hiérarchique des répertoires.
- Par ailleurs, le NameNode va savoir quels blocs sont sur quels serveurs.
- Ainsi, il va savoir que le bloc A est présent à la fois sur le DataNode 1 et 2, le bloc B sur le DataNode 1 et 3, etc…
- Toutes les métadonnées sont stockées physiquement sur le disque système dans deux types de fichiers spécifiques : edits_xxx et fsimage_xxx.
- Chaque cluster comporte un Namenode individuel faisant office de serveur principal. Ainsi, les clients peuvent accéder aux bonnes données au bon moment. Le Namenode se charge également d’ouvrir, fermer, renommer les fichiers ou même les dossiers. Chaque nœud comporte également un ou plusieurs Datanode, auquel est assignée la tâche de gérer le stockage associé au nœud. Les blocs sont cartographiés par le Namenode pour les Datanodes.

![working-hdfs](assets/working-hdfs.jpeg)

### **La communication**

- Il y a, une communication permanente entre les DataNodes et le NameNode afin d'informer les uns et les autres de qui possède quel bloc.
- Le NameNode stocke ces données-là, les métadatas dans un fichier appelé le FS Image et va stocker les modifications au fur et à mesure de la vie du NameNode dans un EditLog qui est un journal des modifications.
- Ce dernier a tendance à grossir avec la vie du NameNode et il est mergé directement dans le FS Image au démarrage du NameNode.
- Le NameNode peut se mettre en SafeMode :
  - Dans ce cas-là, c'est un mode simplement où tout est accessible uniquement en lecture seule, qui se met généralement soit au démarrage,
  - Le temps qu'il se connecte à tous les DataNodes, ou à la demande d'un administrateur ou en cas de soucis afin de faciliter le débogage.
  - Il possède également un mode Recovery interactif permettant de contrôler plus précisément ce qui se fait dessus.

### **NameNode (rôle résumé)**

- La position des blocs dans les Data nodes est reconstruite à chaque démarrage du Namenode dans un mode appelé safe mode.
- Le safe mode bloque l'écriture sur HDFS, car le Name node charge les fichiers edits_xxx et fsimage_xxx et attend le retour des Data nodes sur la position des blocs.
- Le Name node charge tout en mémoire, chaque fichier, répertoire et bloc dans HDFS est représenté comme un objet dans la mémoire et occupe 150 octets.

### **DataNode**

- Il contient les blocs de données et sont surnommés les Workers.
- Les Data nodes sont sous les ordres du Name Node:
- En lecture, les Data nodes vont transmettre au client les blocs correspondant au fichier à transmettre.
- En écriture, les Data nodes vont retourner l'emplacement des blocs fraîchement créés.
- Les Data nodes sont également sollicités lors de l'initialisation du Name node et aussi de manière périodique, afin de retourner la liste des blocs stockés.

### **Secondary NameNode**

- Il vérifie périodiquement l'état du Name Node principal et **agit comme « checkpoint »** : il fusionne les fichiers `edits_xxx` dans `fsimage_xxx` (voir les schémas ci-dessous), allégeant le journal du NameNode.
- ⚠️ **Le NameNode secondaire n'est PAS un NameNode de secours.** Si le NameNode principal tombe, le secondaire **ne prend pas sa place** : le cluster redevient disponible seulement après le redémarrage du NameNode principal (ou via la **haute disponibilité** — un *standby NameNode* actif piloté par ZooKeeper, hors périmètre de notre petit cluster). Reprenez le schéma de la séquence 01 : la redondance « active » du NameNode passe par le failover HA, pas par le SNN.

![fsimages-edit](assets/fsimages-edit.jpeg) ![managing-failures](assets/managing-failures.jpeg)

![fsimage-edits-2](assets/fsimage-edits-2.png) ![checkpointing](assets/checkpointing.png)

---

## **Opérations HDFS**

![operation-1](assets/operation-1.png)
![operation-2](assets/operation-2.png)

- Quelques exemples d’opérations :
- **Démarrage de HDFS**
  - Sur une installation vierge, vous devez d'abord formater le système de fichiers HDFS configuré, depuis le namenode (serveur HDFS) :
    **hdfs namenode -format**
  - > Sur le cluster du cours, ce formatage est déjà fait au premier démarrage du conteneur (entrypoint) — à ne **jamais** relancer, sous peine de perdre les données.
  - Après avoir formaté le HDFS, démarrez le système de fichiers distribué. La commande suivante démarrera le namenode ainsi que les nœuds de données en tant que cluster.
    **start-dfs.sh** (dans le cluster du cours : `./start-dfs.sh` depuis le master)
- **Liste des fichiers dans HDFS**
  - Après avoir chargé les informations dans le serveur, nous pouvons trouver la liste des fichiers dans un répertoire, l'état d'un fichier, en utilisant ‘**ls**’. Voici la syntaxe de **ls** que vous pouvez passer à un répertoire ou à un nom de fichier comme argument.
    **$HADOOP_HOME/bin/hadoop fs -ls <args>**
- **Insertion de données dans HDFS**
  - Supposons que nous ayons des données dans le fichier appelé file.txt dans le système local qui doivent être enregistrées dans le système de fichiers hdfs. Suivez les étapes ci-dessous pour insérer le fichier requis dans le système de fichiers Hadoop.
  1. Étape 1
      - Vous devez créer un répertoire d'entrée.
      **$HADOOP_HOME/bin/hadoop fs -mkdir /user/exemple1**
  2. Étape 2
      - Transférez et stockez un fichier de données des systèmes locaux vers le système de fichiers Hadoop à l'aide de la commande put.
       **$HADOOP_HOME/bin/hadoop fs -put /home/file.txt /user/exemple1**
  3. Étape 3
      - Vous pouvez vérifier le fichier à l'aide de la commande ls.
      **$HADOOP_HOME/bin/hadoop fs -ls /user/exemple1**
- **Récupération de données depuis HDFS**
  - Supposons que nous ayons un fichier en HDFS appelé outfile. Vous trouverez ci-dessous une démonstration simple pour récupérer le fichier requis à partir du système de fichiers Hadoop.
    1. Étape 1
        - Au départ, affichez les données de HDFS en utilisant `cat`.
      **$HADOOP_HOME/bin/hadoop fs -cat /user/output/outfile**
    2. Étape 2
        - Récupérez le fichier de HDFS vers le système de fichiers local en utilisant `get`.
      **$HADOOP_HOME/bin/hadoop fs -get /user/output/ /home/hadoop_tp/**
- **Arrêt HDFS**
  - Vous pouvez arrêter le HDFS à l'aide de la commande suivante.
    **stop-dfs.sh**

---

## **Configurer et démarrer HDFS**

### **Initialiser et démarrer**

1. Initialiser le système de fichiers HDFS
    - `hdfs namenode -format` ne configure pas le NameNode : elle **initialise** son répertoire de métadonnées (le `fsimage`).
    - À ne lancer **qu'une seule fois**, avant le premier démarrage : la relancer efface le namespace et désaligne le `clusterID` des DataNodes. ⚠️ Sur le cluster du cours, ce formatage est déjà fait : ne le relancez jamais.

2. Démarrer HDFS
    - `start-dfs.sh` démarre les démons HDFS : le **NameNode** (master) et les **DataNodes** (nœuds de stockage).

3. Démarrer YARN
    - `start-yarn.sh` démarre les démons YARN : le **ResourceManager** (master) et les **NodeManagers** (workers).

4. Interface web HDFS
    - L'interface web du **NameNode** écoute par défaut sur le port **9870** (**50070** avant Hadoop 3).
    - On y parcourt le système de fichiers, les blocs et les DataNodes.
    - <http://localhost:9870/>

5. Interface web YARN
    - L'interface web du **ResourceManager** écoute par défaut sur le port **8088** : on y suit les applications du cluster.
    - **<http://localhost:8088/>**

 ![hadoop-webviews](assets/hadoop-webviews.png)

---

### **Démarrer le cluster HDFS**

- Une fois la configuration effectuée, on peut démarrer les démons HDFS : le **NameNode** et les **DataNodes**, afin d'accéder à HDFS.

- Les scripts de démarrage/arrêt des démons se trouvent dans le répertoire `sbin` de l'installation Hadoop (par exemple `/opt/hadoop/sbin` sur le cluster du cours).

| Script | Effet |
|---|---|
| `start-dfs.sh` | démarre HDFS (NameNode + DataNodes) sur Linux |
| `start-dfs.cmd` | équivalent Windows |
| `stop-dfs.sh` | arrête HDFS |

> **En production :** plutôt que d'exécuter les démons sous `root`, la bonne pratique est de créer un utilisateur dédié **hdfs**, lui donner les permissions sur les répertoires Hadoop, et exécuter les démons sous ce compte. Dans notre environnement de TP, on reste en `root` pour simplifier.

- Le démarrage se fait donc par `./start-dfs.sh` (déjà connecté en `root` dans notre environnement de TP — pas de `sudo` ni de mot de passe ici).
- Ces manipulations seront pratiquées lors des exercices.

---

[← Chapitre précédent](02-presentation-env-hadoop.md) · [Sommaire](Readme.md) · [Chapitre suivant →](04-mapreduce-et-yarn.md)
