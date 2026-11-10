![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# **Présentation de l'environnement Hadoop**

> **Objectifs** : comprendre l'architecture de l'environnement de TP (cluster Docker local, conteneurs, services) · connaître les scripts de manipulation et leur usage.
> **Prérequis** : séquence 01 · **Durée** : ~2 h · **Type** : cours (environnement)
> **Mise en pratique** : [exercices/02 — Lancement de Hadoop](exercices/02-lancement-de-hadoop.md)

## **Sommaire**

- [Où se trouve l'environnement Hadoop ?](#où-se-trouve-lenvironnement-hadoop-)
- [Architecture de l'environnement](#architecture-de-lenvironnement)
- [Scripts de configuration & manipulation](#scripts-de-configuration--manipulation)
- [Accéder à l'environnement à distance](#accéder-à-lenvironnement-à-distance)

---

## **Où se trouve l'environnement Hadoop ?**

---

L'environnement de TP est un **cluster Docker local** : il tourne sur la machine du formateur ou sur votre propre poste, piloté par Docker Compose. Il se compose de **quatre conteneurs** — un master Hadoop, deux slaves, et un conteneur NiFi — construits à partir d'un projet de déploiement fourni avec le cours.

Contrairement à un vrai cluster, tout tient sur une seule machine : les mêmes logiciels (HDFS, YARN, HBase, Hive), les mêmes configurations, à l'échelle de votre portable. C'est le compromis classique de la formation : **la fidélité des outils, l'échelle du laboratoire**.

La procédure complète (lancer les conteneurs, démarrer les services, ouvrir les interfaces web) est détaillée dans l'exercice 02. La section [« Accéder à l'environnement à distance »](#accéder-à-lenvironnement-à-distance) couvre le cas d'un cluster hébergé sur une VM distante.

---

## **Architecture de l'environnement**

---

### **Pourquoi Docker ?**

Docker permet de créer facilement des conteneurs et des applications basées sur les conteneurs. On les utilise ici pour virtualiser notre environnement Hadoop :

* Un environnement d'exécution **léger**, alternative aux méthodes de virtualisation traditionnelles basées sur les machines virtuelles ;
* Le conteneur **partage le kernel de son OS** avec d'autres conteneurs ;
* Un conteneur est plus léger et plus simple qu'une machine virtuelle.

### **L'environnement Hadoop de Diginamic**

L'environnement est virtualisé avec Docker et se compose de :

* Un réseau nommé **hadoop-net**
* **Trois conteneurs de cluster** (1 master & 2 slaves), image : `hadoop-cluster-base` (Debian 12)
    - **Hadoop 3.3.6** · HBase 2.5.15 · ZooKeeper 3.8.6 · Hive 4.1.0 · Java 17 · Python 3.11
    - Volumes Docker pour les données HDFS (elles survivent aux redémarrages)
    - Redirection de ports nécessaires pour accéder aux interfaces web de Hadoop
* **Un conteneur NiFi** (séquence 07), image officielle `apache/nifi` 2.x, sur le même réseau

```
┌──────────────────────────────────────────────────────────┐
│              Machine hôte (Docker Compose)               │
│                                                          │
│  ┌────────────────┐  ┌────────────────┐                  │
│  │ hadoop-master  │  │ hadoop-slave1  │ + slave2        │
│  │ NameNode       │  │ DataNode 1     │  idem 2         │
│  │ + HBase Master │  │ + RegionServer │                  │
│  │ + HiveServer2  │  │ + NodeManager  │                  │
│  └────────────────┘  └────────────────┘                  │
│   réseau 'hadoop-net'                                    │
│   services : HDFS · YARN · Hive · HBase · Thrift · ZK    │
│                                                          │
│  ┌────────────────────┐                                  │
│  │  conteneur NiFi    │  (réseau hadoop-net, séq. 07)   │
│  │  UI : port 8443    │  (https, certificat auto-signé) │
│  └────────────────────┘                                  │
└──────────────────────────────────────────────────────────┘
        ▲ http://localhost:9870 (HDFS) · :8088 (YARN) · https://localhost:8443/nifi …
        │
   votre navigateur
```

> **Note sur les versions :** le cluster local tourne sur **Hadoop 3.3.6** (et non 2.x) : les ports d'interface ont changé entre les deux branches — l'UI du NameNode est sur le **9870** en 3.x (50070 en 2.x). Les cours et exercices utilisent les ports 3.x.

> **Note sur NiFi :** NiFi vit dans son propre conteneur (image officielle `apache/nifi`, branche **2.x** — Java 21 et client Hadoop 3.x, cohérents avec notre Hadoop 3.3.6). Aucun script à lancer : il démarre avec les conteneurs et se pilote à l'interface web. Détails en cours 07, section « NiFi dans l'environnement du cours ».

### **Configurations logicielles**

| Composant | Version | Remarque |
|---|---|---|
| Hadoop | 3.3.6 | HDFS + YARN + MapReduce |
| HBase | 2.5.15 | NoSQL colonnes (séquence 06) |
| Hive | 4.1.0 | SQL sur HDFS (séquence 05) |
| ZooKeeper | 3.8.6 | coordination HBase |
| Java | OpenJDK 17 | tous les services |
| **Python** | **3.11** | mappers/reducers, HappyBase, pandas… |
| NiFi | 2.x | ingestion (séquence 07, conteneur dédié) |

Python **3.11** supporte les syntaxes modernes (f-strings y compris). Les bibliothèques utiles sont préinstallées :

| Bibliothèque | Usage |
|---|---|
| happybase | interagir avec HBase depuis Python |
| matplotlib | visualisation, export PDF |
| pandas | manipulation de données |
| thriftpy2 | support Thrift pour happybase |

---

## **Scripts de configuration & manipulation**

---

Plusieurs scripts facilitent les manipulations — deux familles : ceux de **l'hôte** (votre machine, au niveau Docker) et ceux **du master** (dans le conteneur, au niveau des services Hadoop).

> Je recommande de **regarder le contenu des scripts** pour voir les vraies commandes lancées.

### **Sur la machine hôte (Docker)**

| Script / commande | Rôle | Utilisé quand |
|---|---|---|
| `docker compose up -d` | build (si besoin) et démarre les 4 conteneurs | début de chaque séance de TP |
| `docker compose down` | arrête les conteneurs (les volumes persistent) | fin de séance |
| `./bash_hadoop_master.sh` | ouvre un terminal bash dans le conteneur master | dès qu'il faut lancer des commandes Hadoop |
| `docker exec -it hadoop-slave1 bash` | terminal bash dans un slave | vérifier les services sur les slaves |

> Les conteneurs démarrent **froids** : SSH est actif, mais **aucun service Hadoop n'est lancé**. C'est voulu — les services se démarrent à la main depuis le master, et vous apprendrez à le faire à l'exercice 02.

### **Dans le conteneur master (dans /home)**

| Script | Rôle | Utilisé quand |
|---|---|---|
| `start-all.sh` | démarre tous les services dans l'ordre (ZK → HDFS → YARN → Historique → HBase → Thrift → REST → Hive) | raccourci une fois les ordres compris |
| `start-zookeeper.sh` | démarre ZooKeeper | avant HBase |
| `start-dfs.sh` | démarre HDFS (NameNode + DataNodes) | début de toute séance |
| `start-yarn.sh` | démarre YARN (ResourceManager + NodeManagers) | EX MapReduce / Hive |
| `start-hbase.sh` | démarre HBase (Master + RegionServers) | EX HBase (séquence 06) |
| `start-thrift.sh` | démarre le service Thrift d'HBase (port 9090) | EX HappyBase (séquence 06) |
| `start-rest.sh` | démarre l'API REST d'HBase (port 9091) | connexion Power BI (annexe) |
| `start-hive.sh` | démarre HiveServer2 (port JDBC 10000) | EX Hive (séquence 05) |

> **NiFi : aucun script à lancer** — le conteneur démarre avec le compose et son interface est auto-suffisante (séquence 07).

> **Vérifier ce qui tourne** : dans le master ou un slave, la commande `jps` liste les processus Java actifs (NameNode, DataNode, ResourceManager, HMaster, ThriftServer, HiveServer2…).

### **Arrêter les services**

Depuis le master : `pkill -f '<nom du service>'` (ex. `pkill -f 'NameNode'`), puis `docker compose down` sur l'hôte. L'ordre des services importe peu pour l'arrêt.

---

## **Accéder à l'environnement à distance**

---

Le cluster décrit ci-dessus tourne **localement** (formateur ou poste apprenant). Pour une session où le cluster serait hébergé sur une **VM distante** (VPN, salle de formation, cloud), seuls les points d'accès changent :

* **Interfaces web** : remplacer `localhost` par l'adresse de la VM (`http://<IP_VM>:9870`, etc.) — les ports restent les mêmes, à condition qu'ils soient ouverts sur la VM ;
* **Terminal master** : `ssh <utilisateur>@<IP_VM>` puis les mêmes scripts `docker exec` / `bash_hadoop_master.sh` ;
* **Transferts de fichiers** : `scp` ou FileZilla (SFTP) vers la VM, même destination.

Le guide complet de déploiement sur VM est fourni avec le projet Docker du cours (`docs/deploiement-vm.md`) : prérequis, build, ports, vérifications. Dans tous les cas, **l'adresse exacte vous est communiquée par votre formateur en début de séance** — hors session locale, tout le reste de ce cours s'applique à l'identique.
