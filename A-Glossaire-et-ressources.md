[← Chapitre précédent](07-nifi.md) · [Sommaire](Readme.md)

![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# Glossaire et ressources

> **Nature :** annexe de référence · **Usage :** tout au long du cours
> Chaque sigle et chaque notion du cours sont définis à leur première occurrence dans le chapitre indiqué, puis retrouvés ici. Les définitions sont volontairement courtes : c'est le chapitre qui les travaille.

## Acronymes et termes

| Terme | Signification | Où on le rencontre |
|---|---|---|
| ACID | Atomicité, Cohérence, Isolation, Durabilité : les garanties transactionnelles qu'un format de table comme Iceberg apporte au stockage de fichiers | chap. 05 |
| ACL (*Access Control List*) | Permissions fines du modèle POSIX, posées par `setfacl` et lues par `getfacl` — distinctes du trio `rwx` | chap. 03 |
| Airflow (Apache) | Ordonnanceur de workflows, devenu le standard de fait de l'orchestration | chap. 01, 07 |
| API (*Application Programming Interface*) | Ce par quoi on appelle un service : l'API Java d'HDFS, l'API REST d'HBase, la Provenance API de NiFi | chap. 03, 06, 07 · TD 04 |
| ApplicationMaster (AM) | Démon propre à chaque application YARN : il négocie les conteneurs et orchestre les tâches de son application | chap. 04 |
| Back pressure (NiFi) | Mécanisme par lequel une file pleine arrête proprement le processeur amont au lieu de le submerger | chap. 07 · EX 07 · TD 04 |
| Batch (traitement par lots) | Traitement séquentiel d'un volume de données sans interaction utilisateur — le régime de MapReduce et de Hive | chap. 04, 06 |
| Beeline | Client officiel de Hive, en JDBC (`beeline -u jdbc:hive2://localhost:10000`) ; la CLI `hive` a disparu de Hive 4 | chap. 05 · EX 05 · TP |
| Big Data | Données trop massives, distribuées et hétérogènes pour l'architecture centralisée classique | chap. 01, 06 |
| Bloc (*block*) | Unité de stockage HDFS — 128 Mio par défaut sur le cluster ; un fichier est découpé en blocs répliqués | chap. 01, 03, 04 |
| Bucket (Hive) | Répartition des lignes d'une table en un nombre fixé de fichiers selon le hash d'une colonne | chap. 05 |
| Bureau à distance | Accès graphique à la VM depuis Windows, pour travailler sans terminal | annexe `Bureau-a-distance.pdf` |
| Canvas (NiFi) | Surface de travail de l'interface, où l'on dépose et câble les processeurs | chap. 07 |
| Checkpoint | Opération qui fusionne la FSImage et l'EditLog pour borner la taille du journal | chap. 03 |
| CLI (*Command Line Interface*) | Interface en ligne de commande | chap. 05 |
| Combiner | « Mini-reducer » exécuté côté mapper avant le shuffle pour alléger le réseau ; il ne sait pas moyenner | chap. 04 |
| Connection (NiFi) | File d'attente à capacité limitée entre deux processeurs — la *queue* ; elle porte le nom de la *relationship* | chap. 07 |
| Conteneur (Docker) | Processus isolé avec son propre système de fichiers ; le cluster du cours tient dans trois conteneurs | chap. 02 |
| Controller service (NiFi) | Service partagé, configuré une fois et référencé par plusieurs processeurs (le format CSV↔TSV, par exemple) | TD 04 · EX 07 |
| ConvertRecord (NiFi) | Processeur qui lit le contenu comme des enregistrements et le réécrit dans un autre format (CSV → TSV) | chap. 07 · EX 07 · TP |
| CSV (*Comma-Separated Values*) | Fichier tabulaire à séparateur virgule ; format d'origine des jeux de données du cours | chap. 04 · EX 04 · TP |
| Data lake | Magasin de fichiers bruts, où le schéma n'est lu qu'à la lecture | chap. 05, 07 |
| DataNode | Démon qui stocke les blocs sur chaque nœud esclave et exécute les ordres du NameNode | chap. 01, 02, 03 |
| Data Provenance (NiFi) | Historique requêtable de chaque FlowFile : heure, processeurs traversés, attributs, contenu avant/après | chap. 07 · TD 04 · TP |
| Data warehouse (entrepôt de données) | Magasin optimisé pour l'analyse en batch de gros volumes — le statut de Hive, qui ne stocke rien lui-même | chap. 05 |
| Derby (Apache) | Base relationnelle embarquée : le Metastore par défaut de Hive | chap. 05 |
| Distribution Hadoop | Version packagée du socle avec ses outils et son support — Cloudera (CDP), Hortonworks, MapR | chap. 01 |
| Docker | Moteur de conteneurs qui fait tourner le cluster sur votre machine | chap. 02 |
| Docker Compose | Outil qui démarre d'un coup plusieurs conteneurs décrits dans un même fichier YAML | chap. 02, 07 |
| EditLog | Journal de toutes les modifications de l'arborescence HDFS | chap. 03 |
| ETL (*Extract, Transform, Load*) | Extraire, transformer, charger — le métier de NiFi en version légère | chap. 05, 07 |
| EX | Exercice d'application d'une séquence, guidé puis autonome | exercices 02 à 07 |
| EXPLAIN | Commande Hive qui affiche le plan d'exécution (les *stages*) généré à partir de votre requête SQL | chap. 05 · EX 05 · TP |
| Famille de colonnes (*column family*) | Groupement de colonnes déclaré à la création d'une table HBase ; il organise le stockage physique | chap. 06 |
| FIFO / Fair / Capacity Scheduler | Politiques d'ordonnancement de YARN : premier arrivé premier servi, partage équitable, quotas par file | chap. 04 |
| FileZilla | Client graphique FTP/SFTP pour déposer des fichiers sur la VM | chap. 02 · annexe `Filezilla.pdf` |
| FlowFile (NiFi) | Unité de données en mouvement : un contenu (le fichier) et des attributs (métadonnées clé/valeur) | chap. 07 · TD 04 |
| FSImage | Image de l'arborescence HDFS, chargée au démarrage du NameNode | chap. 03 |
| Funnel (NiFi) | Exutoire qui fusionne plusieurs flux entrants en une seule file — ce n'est pas un processeur | chap. 07 |
| GetFile (NiFi) | Processeur source : tout fichier apparaissant dans un répertoire devient un FlowFile | chap. 07 · EX 07 · TP |
| HA (haute disponibilité) | Capacité du cluster à continuer de servir malgré la panne d'un nœud ; pour le NameNode, *failover* actif piloté par ZooKeeper — hors périmètre de notre cluster | chap. 01, 03, 06 |
| Hadoop | Framework Apache de stockage et de calcul distribués sur des machines standard | chap. 01 |
| HadoopFS | Couche d'abstraction de fichiers d'Hadoop : le même code accède HDFS, le disque local, S3, Swift… | chap. 03 |
| Hadoop Streaming | Mécanisme qui fait d'un programme quelconque (Python, Bash…) un mapper ou un reducer, via `stdin` et `stdout` | chap. 04 · EX 04 · TP |
| HappyBase | Bibliothèque Python qui pilote HBase par l'intermédiaire de Thrift | chap. 06 · EX 06 · TP |
| HBase | Base NoSQL orientée colonnes bâtie sur HDFS, pour les lectures et écritures aléatoires à la ligne | chap. 06 |
| HDFS (*Hadoop Distributed File System*) | Système de fichiers distribué d'Hadoop : blocs, réplication, nommage centralisé | chap. 03 |
| HFile | Fichier persistant de HBase, stocké sur HDFS | chap. 06 |
| Hive | Entrepôt de données SQL au-dessus de Hadoop : il compile le HiveQL en jobs MapReduce | chap. 05 |
| HiveQL | Le dialecte SQL de Hive | chap. 05 |
| HiveServer2 | Service de Hive qui reçoit les requêtes et héberge le driver ; écoute sur le port 10000 | chap. 05 |
| HMaster (HBase Master) | Démon qui assigne les régions, supervise les RegionServers et gère les schémas ; un seul est actif | chap. 06 |
| Iceberg (Apache) | Format de table *lakehouse* — transactions ACID, évolution de schéma, *time travel* — intégré à Hive 4 | chap. 01, 05 |
| JDBC (*Java Database Connectivity*) | API Java de connexion aux bases ; `jdbc:hive2://localhost:10000` est celle de Hive | chap. 05 |
| JDK (*Java Development Kit*) | Boîte à outils Java ; `JAVA_HOME` l'indique à Hadoop | chap. 01 |
| JNI (*Java Native Interface*) | Pont qui autorise du code natif — celui d'un plugin — à s'exécuter dans la JVM d'un démon Hadoop | chap. 03 |
| JobHistoryServer | Démon qui conserve l'historique des jobs MapReduce et leurs journaux après la fin de l'application | EX 02 |
| jps | Commande qui liste les processus Java d'une machine : c'est ainsi qu'on vérifie que les démons sont démarrés | chap. 02, 06 · EX 02 |
| JVM (*Java Virtual Machine*) | Machine virtuelle qui exécute démons et tâches ; chaque tâche map ou reduce lance sa JVM | chap. 01, 04 |
| Kafka (Apache) | Bus de flux de messages ; Kafka Connect est l'outil d'ingestion quand la source est un flux | chap. 01, 07 |
| Kerberos | Protocole d'authentification fort, optionnel dans Hadoop | chap. 03 |
| Lakehouse | Architecture qui marie le stockage de fichiers du *data lake* et les garanties d'un entrepôt | chap. 05, 07 |
| Lineage (NiFi) | Graphe des processeurs traversés par un FlowFile, affiché dans Data Provenance | TD 04 · EX 07 · TP |
| Mapper et reducer | Programme qui émet les paires `clé\tvaleur` à partir d'un split / programme qui agrège les valeurs d'une même clé | chap. 04 · EX 04 |
| MapReduce (MR) | Modèle de programmation parallèle du framework : map → partition → shuffle/sort → reduce | chap. 04 |
| MasterNode / SlaveNode | Rôles des machines d'un cluster : le master porte la coordination, les slaves portent le stockage et le calcul | chap. 01 |
| MemStore | Tampon d'écriture en mémoire d'un Store HBase, vidé en HFiles | chap. 06 |
| Metastore | Catalogue de Hive : tables, colonnes, formats, emplacements — aucunement les données | chap. 05 |
| MySQL | Base relationnelle recommandée pour le Metastore dès que Hive est en production | chap. 05 |
| NAR (*NiFi Archive*) | Paquet de processeurs chargeable par NiFi ; l'image du cours embarque 118 NAR, dont aucun NAR Hadoop | chap. 07 · EX 07 |
| NameNode | Maître de HDFS : il connaît l'arborescence, les blocs et les réplicas, et décide de la réplication | chap. 01, 03 |
| NameNode secondaire (SNN) | Assistant qui factorise l'EditLog et tient une copie de la FSImage — ce n'est **pas** un NameNode de secours | chap. 03 |
| Namespace (HBase) | Groupement logique de tables, équivalent d'une base en SGBDR | chap. 06 |
| NiFi (Apache) | Plateforme visuelle de routage et de transformation de flux de données, avec livraison garantie et provenance | chap. 07 |
| NodeManager | Démon YARN de chaque nœud : il surveille les conteneurs et rend compte au ResourceManager | chap. 02, 04 |
| NoSQL | Familles de bases non relationnelles — clé/valeur, documents, colonnes, graphes — pour les volumes et la distribution | chap. 06 |
| ODBC (*Open Database Connectivity*) | API de connexion aux bases indépendante du langage ; c'est par elle que Power BI interroge des données | annexe `ODBC.pdf` |
| OLAP / OLTP | Analyse en lecture sur de gros volumes / transactions à la ligne en temps réel | chap. 05 |
| Oozie | Ordonnanceur de workflows propre à Hadoop, en mode maintenance | chap. 01 |
| Partition (Hive) | Sous-dossier correspondant à une valeur de colonne ; un filtre sur la partition ne lit que ce dossier (*partition pruning*) | chap. 05 |
| Partition (MapReduce) | Étape qui route chaque paire intermédiaire vers le reducer qui l'agrège | chap. 04 |
| Pig | Flux de données « SQL-like » sur Hadoop, aujourd'hui legacy | chap. 01 |
| Power BI | Outil de visualisation de Microsoft, connectable à HBase via ODBC et l'API REST | chap. 02 · annexe `ODBC.pdf` |
| Processeur (NiFi) | Brique de traitement du flux : source, transformation, routage ou destination | chap. 07 · TD 04 |
| PutFile (NiFi) | Processeur de destination : il écrit le contenu du FlowFile sur le disque du conteneur | chap. 07 · EX 07 · TP |
| PuTTY | Client SSH graphique pour Windows | annexe `PuTTY.pdf` |
| PutHDFS / GetHDFSFile | Processeurs NiFi de aller-retour direct avec HDFS — absents de l'image du cours, d'où le passage par `hdfs dfs -moveFromLocal` | chap. 07 · EX 07 |
| Relationship (NiFi) | Sortie nommée d'un processeur, `success` ou `failure` ; une relationship non câblée est une erreur de conception | chap. 07 · TD 04 |
| Région (HBase) | Partition horizontale d'une table HBase, unité de distribution et de disponibilité — 1 Go ici | chap. 06 |
| RegionServer | Démon HBase qui héberge les régions et exécute les lectures et les écritures | chap. 06 |
| ResourceManager (RM) | Démon YARN du master : il alloue les ressources et planifie les applications | chap. 02, 04 |
| REST (API REST) | API exposée en HTTP ; celle d'HBase écoute sur le port 9091 | chap. 02, 06 |
| RowKey | Clé de ligne HBase, unique, triée en ordre lexicographique | chap. 06 |
| S3 (*Simple Storage Service*) | Stockage d'objets d'Amazon ; un des backends accessibles derrière HadoopFS | chap. 03, 05 |
| SafeMode | Mode dégradé du NameNode au démarrage : pas d'écriture tant que les DataNodes n'ont pas déclaré leurs blocs | chap. 03 |
| Scaling out / upsizing | Ajouter des machines standard / muscler une machine existante — les deux réponses opposées à la croissance | chap. 01, 06 |
| SCP (*Secure Copy*) | Copie de fichier par SSH | chap. 02 · EX 02 · annexe `SCP.pdf` |
| SFTP | FTP tunnelisé par SSH — ce que fait FileZilla | chap. 02 |
| SGBDR | Système de gestion de base de données relationnelles | chap. 01, 06 |
| Shuffle / sort | Étape qui déplace les paires vers les reducers puis les trie par clé | chap. 04 |
| Spark (Apache) | Moteur de calcul distribué en mémoire ; Spark SQL et Spark JDBC appartiennent au paysage actuel | chap. 01, 05, 07 |
| Split | Tranche de lecture attribuée à un mapper — un split = une tâche map | chap. 04 |
| Sqoop | Transfert HDFS ↔ bases relationnelles ; projet retiré vers l'Attic d'Apache en 2021 | chap. 01, 07 |
| SSH (*Secure Shell*) | Connexion chiffrée à distance d'une machine, port 22 | chap. 02 · annexe `SSH.pdf` |
| Store (HBase) | Une famille de colonnes vue physiquement : un MemStore et des HFiles | chap. 06 |
| TD | Travaux dirigés de premier contact : ils précèdent la séquence suivante et en montrent le principe sur les vrais outils | TD 01 à 05 |
| Tez | Moteur d'exécution que Hive peut substituer à MapReduce | chap. 05 |
| Thrift | Passerelle réseau multi-langages ; HBase l'expose sur le port 9090 pour HappyBase | chap. 05, 06 |
| TP | Travaux pratiques : le projet de synthèse évalué, qui enchaîne toute la chaîne | TP |
| Trino | Moteur SQL interactif qui lit les mêmes fichiers HDFS via le même Metastore | chap. 05 |
| TSV (*Tab-Separated Values*) | Fichier tabulaire à séparateur tabulation ; c'est le format que lisent les tables Hive du cours | chap. 05, 07 · EX 05 · TP |
| UI (*User Interface*) | Interface web d'un service : 9870 pour HDFS, 8088 pour YARN, 8443 pour NiFi | chap. 02, 04, 07 |
| URI / URL | Adresse d'un chemin : avec protocole (`hdfs://…`) ou résolu par défaut par HadoopFS | chap. 03 |
| VM (*Virtual Machine*) | Machine virtuelle qui héberge Docker et donc tout le cluster | chap. 02 |
| WAL (*Write-Ahead Log*) | Journal d'écriture de HBase : toute écriture y est tracée avant d'atteindre les HFiles | chap. 06 |
| YARN (*Yet Another Resource Negotiator*) | Gestionnaire de ressources du cluster : il découple l'allocation des ressources et l'exécution des applications | chap. 04 |
| ZooKeeper (ZK) | Service de coordination distribué : état du cluster, élection du maître HBase ; à démarrer **avant** HBase | chap. 06 |

## Ressources

### Annexes du dépôt

**Documentation — `annexes/docs/`**

| Annexe | Contenu |
|---|---|
| [`Bureau-a-distance.pdf`](annexes/docs/Bureau-a-distance.pdf) | Se connecter à la VM en affichage distant depuis Windows |
| [`Filezilla.pdf`](annexes/docs/Filezilla.pdf) | Transférer des fichiers vers la VM avec FileZilla (SFTP) |
| [`Hadoop.pdf`](annexes/docs/Hadoop.pdf) | Support de synthèse sur l'écosystème Hadoop |
| [`ODBC.pdf`](annexes/docs/ODBC.pdf) | Connecter Power BI à des données du cluster via ODBC |
| [`PuTTY.pdf`](annexes/docs/PuTTY.pdf) | Se connecter à la VM en SSH avec PuTTY |
| [`SCP.pdf`](annexes/docs/SCP.pdf) | Copier des fichiers par SSH (`scp`) |
| [`SSH.pdf`](annexes/docs/SSH.pdf) | Connexion SSH à la VM |

**Scripts de référence — `annexes/utils/`**

- [`0-job.sh`](annexes/utils/0-job.sh) — enchaîner démarrage des services, préparation HDFS, soumission du job et lecture du résultat.
- [`1-structure-mapper.py`](annexes/utils/1-structure-mapper.py), [`2-structure-reducer.py`](annexes/utils/2-structure-reducer.py) — squelettes commentés d'un mapper et d'un reducer.
- [`3-mapper-txt.py`](annexes/utils/3-mapper-txt.py), [`4-mapper-csv-reader.py`](annexes/utils/4-mapper-csv-reader.py) — mapper sur texte simple, puis mapper avec lecture CSV protégée.
- [`5-reducer-hbase.py`](annexes/utils/5-reducer-hbase.py), [`6-reducer-matplotlib-pdf.py`](annexes/utils/6-reducer-matplotlib-pdf.py) — reducer qui écrit dans HBase, reducer qui produit un graphique PDF.
- [`7-prepare-spotify-tsv.py`](annexes/utils/7-prepare-spotify-tsv.py) — préparation du jeu Spotify pour les tables Hive (CSV → TSV).
- [`8-mapper-ventes.py`](annexes/utils/8-mapper-ventes.py), [`9-mapper-ventes-annee.py`](annexes/utils/9-mapper-ventes-annee.py), [`10-reducer-ventes.py`](annexes/utils/10-reducer-ventes.py), [`11-reducer-ventes-hbase.py`](annexes/utils/11-reducer-ventes-hbase.py), [`12-prepare-ventes-tsv.py`](annexes/utils/12-prepare-ventes-tsv.py), [`13-graph-ventes-hbase.py`](annexes/utils/13-graph-ventes-hbase.py) — les scripts du TP final, de la question 1 à la visualisation.

### Environnement technique

Le cluster du cours (Hadoop 3.3.6, Hive 4.1.0, HBase, NiFi 2.11.0) est défini dans le dépôt **[Hadoop](https://github.com/robinhotton/Hadoop)** : `docker-compose.yml`, image `hadoop-cluster-base`, scripts `scripts/master/start-*`.

- Procédure de déploiement : [`docs/deploiement-vm.md`](https://github.com/robinhotton/Hadoop/blob/main/docs/deploiement-vm.md) de ce dépôt, ainsi que `bash_hadoop_master.sh` pour l'accès hôte → master.
- Identifiant et mot de passe de l'interface NiFi : `nifi/credentials.env` (auprès du formateur).

### Interfaces web du cluster

| Adresse | Ce qu'elle montre |
|---|---|
| `http://localhost:9870` | Interface du NameNode — arborescence, `fsck`, SafeMode |
| `http://localhost:9868` | Interface du Secondary NameNode |
| `http://localhost:8088` | Interface du ResourceManager — applications, mappers, reducers, logs |
| `http://localhost:8041` et `http://localhost:8042` | NodeManager des deux slaves — mémoire, conteneurs, tâches |
| `https://localhost:8443/nifi` | Interface de NiFi (HTTPS obligatoire en 2.x) |

### Jeux de données

- `data/Spotify_Most_Streamed_Songs.csv` — fil rouge des exercices 03 à 07.
- `data/dataw_fro03_mini_1000.csv` — support du TP final ; `data/dataw_fro03.csv` (complet) en bonus.

### Liens externes cités dans les chapitres

**Hadoop**

- [Hadoop Unix Shell Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/UnixShellGuide.html) — le rôle de `hadoop-env.sh` (chap. 01).
- [core-default.xml](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/core-default.xml), [hdfs-default.xml](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml), [mapred-default.xml](http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml), [yarn-default.xml](http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml) — toutes les propriétés citées dans le cours y sont définies (chap. 01).
- [Sqoop sur l'Attic d'Apache](https://attic.apache.org/projects/sqoop.html) et [Oozie](https://oozie.apache.org/) — l'état réel de ces projets dits « historiques » (chap. 01).

**HBase**

- [Apache HBase — accueil](https://hbase.apache.org/) et [Reference Guide](https://hbase.apache.org/book.html#_overriding_configuration_starting_the_hbase_shell) (chap. 06).
- [HappyBase — documentation](https://happybase.readthedocs.io/en/latest/) (chap. 06).
- [HBase Shell — Tutorialspoint](https://www.tutorialspoint.com/hbase/hbase_shell.htm) — le répertoire des commandes du shell (chap. 06).

---

[← Chapitre précédent](07-nifi.md) · [Sommaire](Readme.md)
