![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# **NiFi, l'usine à flux de données**

> **Objectifs** : comprendre le problème de l'ingestion de données · maîtriser le modèle dataflow de NiFi (FlowFile, processeurs, connections, back pressure) · découvrir l'interface et la Data Provenance · situer NiFi dans l'écosystème 2026.
> **Prérequis** : séquence 03 (HDFS) · **Durée** : ~3 h · **Type** : cours + démo interactive
> **Mise en pratique** : [exercices/07 — NiFi](exercices/07-nifi.md)

## **Sommaire**

* [NiFi](#nifi)
  * [Le problème : amener les données jusqu'à Hadoop](#le-problème--amener-les-données-jusquà-hadoop)
  * [Présentation de NiFi](#présentation-de-nifi)
  * [Les concepts clés : FlowFile, processeur, connection](#les-concepts-clés--flowfile-processeur-connection)
  * [L'interface : construire un flux à la souris](#linterface--construire-un-flux-à-la-souris)
  * [Data Provenance : tracer chaque événement](#data-provenance--tracer-chaque-événement)
  * [Les processeurs clés pour notre cas d'usage](#les-processeurs-clés-pour-notre-cas-dusage)
  * [NiFi dans l'environnement du cours](#nifi-dans-lenvironnement-du-cours)
  * [NiFi en 2026 : 1.x, 2.x et le paysage de l'ingestion](#nifi-en-2026--1x-2x-et-le-paysage-de-lingestion)
  * [NiFi dans le cursus : la chaîne complète](#nifi-dans-le-cursus--la-chaîne-complète)

---

## **NiFi**

---

### **Le problème : amener les données jusqu'à Hadoop**

Depuis le début de ce cours, vos données arrivent dans HDFS par une seule porte : vous, et votre `hdfs dfs -put`. Réfléchissez à ce que ça donne en entreprise :

- Le CSV de la promotion arrive par mail le lundi, un autre le jeudi — quelqu'un doit le déposer **à la main**, au bon endroit, avec le bon nom.
- Une ingestion qui échoue à 3 h du matin (disque plein, fichier corrompu, réseau coupé) n'est **ni suivie, ni rejouée** : personne ne sait qu'elle a échoué.
- Rien ne trace **qui a déposé quoi, quand, depuis où** — pas d'historique, pas d'audit.
- Multiplier les sources (fichiers, API, flux, bases) multiplie les petits scripts maison, chacun avec ses défauts.

C'est le problème de l'**ingestion** : la porte d'entrée de la plateforme. L'encart « écosystème historique » du cours 01 mentionnait **Sqoop** pour le relationnel — supprimé du cours car désaffecté, remplacé en industrie par Spark JDBC / Kafka Connect. Mais pour l'ingestion de **fichiers** (CSV, logs, JSON), l'outil de référence reste **Apache NiFi**.

### **Présentation de NiFi**

**Apache NiFi** est une plateforme de routage et transformation de données visuelle : on construit des **flux de données** (_dataflows_) en assemblant des blocs à la souris, et NiFi garantit leur livraison.

L'outil a une origine peu banale : développé par la **NSA** (projet « Niagara Files »), open-sourcé en 2014, il rejoint Apache en **2015** sous le nom **NiFi**. Son ADN explique ses forces :

- **Guaranteed delivery** : chaque événement qui entre dans le flux est acké de bout en bout — un processeur en échec ne fait pas perdre la donnée (elle va sur la relation `failure`, pas à la poubelle) ;
- **Back pressure** : si un maillon consomme plus lentement, la pression remonte et le flux ralentit ou s'arrête proprement au lieu de déborder ;
- **Data Provenance** : historique complet et requêtable de chaque événement — à quelle heure, depuis où, par quels processeurs, avec quel contenu — c'est la traçabilité intégrale, clé pour l'audit.

Usages typiques : collecte de logs, ingestion de fichiers vers un datalake (notre cas : CSV → HDFS), synchronisation entre systèmes, ETL léger, IoT (sensor → plateforme).

### **Les concepts clés : FlowFile, processeur, connection**

Le modèle mental de NiFi tient en trois objets — le reste en découle :

**1. Le FlowFile** — l'unité de données en mouvement. Deux parties :

- le **contenu** (le fichier CSV, l'enregistrement JSON...) ;
- les **attributs** (clé/valeur : `filename`, `path`, et vos propres métadonnées — `ingestion_date`, `source`, etc.).

Un FlowFile traverse le flux en gardant son contenu, enrichi d'attributs à chaque étape. _Pensez : une enveloppe (contenu) avec une fiche de renseignement qui se complète en chemin (attributs)._

**2. Le processeur** — une brique de traitement. Il existe 300+ processeurs couvrant quatre familles :

| Famille | Rôle | Exemples |
|---|---|---|
| **Source (ingest)** | faire entrer des données | `GetFile`, `ListenHTTP`, `QueryDatabaseTable` |
| **Transformation** | modifier contenu/attributs | `UpdateRecord`, `ConvertRecord`, `UpdateAttribute` |
| **Routage** | orienter, filtrer | `RouteOnAttribute`, `QueryRecord` |
| **Destination (egress)** | faire sortir | `PutFile`, `PutS3Object`, `PutKafka` |

> Un **funnel** (entonnoir) n'appartient à aucune de ces familles : ce n'est **pas** un processeur. C'est un exutoire, une cible de connection qui accepte plusieurs flux entrants et les fusionne en une seule file — typiquement pour récolter les `failure` de toute une branche sans créer de processeur. Il se dépose depuis la palette comme les connections, et ne s'configure pas.

Un processeur émet son résultat sur des **relationships** : `success` (traité) et `failure` (en échec) le plus souvent. **Une relationship non connectée est une erreur de conception** : elle signifie « si ça échoue, la donnée disparaît silencieusement ».

**3. La connection** — la file entre deux processeurs. C'est une vraie **queue** avec capacité limitée (back pressure : nombre d'objets / volume de données) ; si elle se remplit, le processeur amont est **arrêté** en amont (stop and wait), pas submergé. La connection porte aussi le nom de la relationship (`success`, `failure`).

```
 GetFile ──success──► UpdateRecord ──success──► PutFile
    │                                          │
    └──failure──► (à câbler absolument !)      failure
```

> **Le réflexe de conception** : chaque flux doit définir **que faire des échecs** — une relation `failure` vers un funnel ou un simple `PutFile` dans un dossier `_errors/`. C'est la différence entre un flux d'ingestion et un script fragile.

### **L'interface : construire un flux à la souris**

L'UI web de NiFi présente un **canvas** : on y glisse des processeurs depuis la palette, on les relie en tirant d'un carré de connection à l'autre, on configure chaque processeur via son panneau **Properties** (paramètres) / **Settings** (nom, auto-terminating relationships) / **Scheduling** (fréquence) / **comments**.

**Le cycle de vie d'un flux** :

1. **Conception** : déposer les processeurs, câbler les relations success **et** failure ;
2. **Configuration** : remplir les propriétés (chemins, formats, connexions HDFS) ;
3. **Démarrage** : sélectionner tout → ▶ Start — les processeurs passent au vert (run) ;
4. **Supervision** : surveiller les compteurs d'événements sur chaque connection (in/out, files/bytes), les badges rouges (erreurs) ;
5. **Itération** : arrêter, modifier une propriété, redémarrer — le design est vivant.

**La couleur des queues** résume l'état du flux : **vide** (fluide), **jaune** (remplissage — back pressure proche), **rouge** (back pressure atteinte — le flux est stoppé proprement en amont, un maillon consomme trop lentement).

### **Data Provenance : tracer chaque événement**

La fonctionnalité la plus impressionnante de NiFi pour un data engineer : cliquer sur un événement dans une queue → **View Data Provenance** : l'historique complet de ce FlowFile — heure d'entrée, processeurs traversés, attributs à chaque étape, **contenu avant/après chaque transformation** (visualisables et téléchargeables), durée de chaque saut.

Concrètement, quand la question arrive — _« Il manque les données du 12 mars, que s'est-il passé ? »_ — la réponse est à trois clics : chercher l'événement, voir où il s'est arrêté, **rejouer** (replay) la donnée depuis le point de l'échec. C'est ce garantie-là — reprise sur erreur avec traçabilité — qui justifie NiFi par rapport à un cron + `hdfs dfs -put`.

### **Les processeurs clés pour notre cas d'usage**

L'EX 07 construira ce flux :

```
GetFile ──► ConvertRecord/UpdateRecord ──► PutFile ──► hdfs dfs -moveFromLocal
(CSV dans le conteneur)   (CSV → TSV)     (dossier de   (commande depuis le master,
                                           sortie NiFi)   pas un processeur)
```

| Processeur | Rôle dans notre flux |
|---|---|
| **GetFile** | scruter un répertoire local du conteneur NiFi ; tout fichier qui apparaît devient un FlowFile (puis le fichier source est supprimé ou archivé selon la config) |
| **ConvertRecord / UpdateRecord** | lire chaque ligne comme enregistrement, transformer (séparateur virgule → tabulation — la même transformation que le script `annexes/utils/7` de l'EX 05 (Hive), en flux) |
| **PutFile** | écrire le contenu du FlowFile sur le disque du conteneur NiFi (répertoire, nom de fichier, stratégie en cas de doublon) |
| *puis, à la main* `hdfs dfs -moveFromLocal` | le dernier pas entre NiFi et HDFS, fait depuis le master |

> ⚠️ **Pourquoi le dernier maillon n'est pas un processeur.** L'image NiFi du cours (`apache/nifi:2.11.0`, officielle) embarque **118 NAR, dont aucun NAR Hadoop** : les processeurs `PutHDFS`, `GetHDFSFile` et leur *controller resource* de configuration Hadoop sont construits derrière un profil Maven optionnel (`include-hadoop`) que la distribution officielle désactive. Vous ne les trouverez pas dans la palette, et il n'y a rien à « ajouter » : la ressource n'est pas installée. Le montage `nifi/hadoop/` du `docker-compose.yml` reste utile pour `hdfs-site.xml` côté conteneur, mais il ne branchera aucun processeur.
>
> Ce n'est pas une perte pédagogique, c'est un gain : le passage par `PutFile` + `hdfs dfs -moveFromLocal` **vous fait faire le geste HDFS vous-même**, et il rend visible une frontière que NiFi a tendance à cacher — *où finit l'orchestrateur, où commence le système de fichiers*. En production on réintroduirait un connecteur HDFS (ou l'on ferait tourner NiFi sur les nœuds du cluster) ; à ce stade du cursus, savoir nommer cette frontière vaut mieux qu'un processeur de plus.

> **Lien pédagogique** : la transformation CSV→TSV que vous avez scriptée en Python à l'EX 05 (`annexes/utils/7`) existe ici en processeur — même logique, deux outils. L'EX 07 vous fera construire ce flux et déposer le TSV **exactement là où** la table externe Hive de l'EX 05 lit ses données.

### **NiFi dans l'environnement du cours**

NiFi n'est **pas** dans l'image du cluster Hadoop (le conteneur master embarque HDFS, YARN, HBase, Hive — pas d'usine à flux). Il tourne dans un **conteneur dédié** du même `docker-compose.yml` — image officielle `apache/nifi` branche **2.x** :

```
┌────────────── machine hôte (docker compose) ─────────┐
│                                                       │
│  ┌────────────────┐   ┌────────────────────────────┐ │
│  │  Conteneur     │   │  Conteneur NiFi (dédié)    │ │
│  │  Hadoop master │◄──┤  UI : https://localhost:8443│ │
│  │  (HDFS 9000)   │   │  2.x (Java 21 embarqué)    │ │
│  └────────────────┘   └────────────────────────────┘ │
│  + 2 conteneurs slaves …        réseau 'hadoop-net'   │
└───────────────────────────────────────────────────────┘
```

Deux points d'accès :

- **l'UI NiFi** : `https://localhost:8443/nifi` — NiFi 2.x impose le HTTPS. Le navigateur affiche d'abord un avertissement de certificat auto-signé : acceptez-le une fois. **Puis vient un écran de connexion**, et c'est normal : NiFi 2.x active par défaut un fournisseur d'identité *single-user* qui répond `401` à toute requête non authentifiée. Le couple est lu depuis `nifi/credentials.env` du projet de déploiement (`SINGLE_USER_CREDENTIALS_USERNAME` / `SINGLE_USER_CREDENTIALS_PASSWORD`) — demandez-le au formateur. Sans ce fichier, NiFi génère au démarrage un identifiant et un mot de passe **aléatoires**, visibles uniquement dans `docker logs hadoop-nifi`, et ils changent à chaque recréation du conteneur. **Même cause, effet identique** si le mot de passe fait moins de **12 caractères** : la variable est rejetée en silence (`ERROR: Password must be at least 12 characters`) et NiFi retombe sur le couple aléatoire — vérification faite sur `apache/nifi:2.11.0`.
- **les fichiers sources** : déposés dans le répertoire `nifi/input/` du projet de déploiement — monté dans le conteneur sur `/opt/nifi/input` (l'EX vous guide). La sortie du flux, `nifi/output/`, est montée au même endroit **dans le master** sous `/home/staging` : c'est ce qui permet d'enchaîner avec `hdfs dfs -moveFromLocal`.

> **Pourquoi la branche 2.x ?** Le cluster du cours est Hadoop **3.3.6** : NiFi 2.x (Java 21, client Hadoop 3.x) est le couple cohérent. Sur un vieux cluster Hadoop 2.x, on repartirait sur la branche 1.x (1.28.x, client Hadoop 2.x) — c'est la seule divergence qui impose un choix de version.

### **NiFi en 2026 : 1.x, 2.x et le paysage de l'ingestion**

- **NiFi 2.x** (depuis fin 2024) : UI repensée, Java 21 minimum, TLS 1.3, stratégies de load balancing des connections améliorées, support des nouveaux paramètres de sécurité. La branche **1.x est en fin de vie progressive** (1.28.x est la dernière, maintenance uniquement) — on ne la croise plus que sur les stacks Hadoop 2.x legacy.
- **Le paysage de l'ingestion 2026** : NiFi n'est pas seul. **Kafka Connect** domine quand la source est un flux Kafka ; **Airbyte** (open source) cible la sync API/SaaS → entrepôt ; **Spark JDBC** a remplacé Sqoop pour le relationnel. NiFi reste la référence pour le **routage et la transformation visuels** multi-sources, et l'IoT/edge (NiFi MiNiFi).
- **Concurrent historique** : Talend/Pentaho (ETL visuel commercial) — NiFi s'en distingue par son modèle FlowFile/queue orienté streaming continu, sa provenance et son prix (libre).

### **NiFi dans le cursus : la chaîne complète**

Avec NiFi, le cursus couvre la chaîne data de bout en bout :

```
     07 NiFi          03 HDFS            04 MapReduce / 05 Hive       06 HBase
  ┌───────────┐     ┌──────────┐        ┌──────────────────────┐   ┌─────────┐
  │ ingestion │ ──► │ stockage │ ─────► │ calcul (MR batch ou  │◄─┤ service │
  │ routage   │     │ fichiers │        │ SQL)                 │   │ clé/val │
  └───────────┘     └──────────┘        └──────────────────────┘   └─────────┘
```

- **07 (NiFi)** amène et transforme la donnée — c'est lui qui ouvre la chaîne ;
- **03 (HDFS)** stocke ;
- **04 (MapReduce) / 05 (Hive)** calculent — deux langages pour le même moteur ;
- **06 (HBase)** sert les accès temps réel par clé.

Le TP final vous proposera de rejouer l'ingestion d'un dataset par NiFi en ouverture de votre projet de synthèse.
