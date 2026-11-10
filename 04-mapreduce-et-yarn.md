[← Chapitre précédent](03-hdfs.md) · [Sommaire](Readme.md) · [Chapitre suivant →](05-hive.md)

![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# 04 - MapReduce et YARN

> **Objectifs** : modéliser le pipeline MapReduce (découpage bloc → split, map → partition → shuffle/sort → reduce) · connaître le contrat d'exécution (processus par tâche, tri, partition) · développer un MapReduce en Python via Hadoop Streaming · tester en local, lancer, suivre et diagnostiquer un job sur le cluster.
> **La vérité vient de la mesure** : tous les chiffres de ce chapitre (nombre de splits, de reducers, latences, compteurs) ont été reproduits sur le cluster du cours (Hadoop 3.3.6). Quand une règle de la littérature ne colle pas à notre cluster, c'est la mesure qui fait foi.
> **Mise en pratique** : [Exercices](#v-exercices)
## Sommaire

- [I. Temps 1 — Le modèle](#i-temps-1--le-modèle)
  - [A. Le pipeline complet](#a-le-pipeline-complet)
  - [B. Du fichier au bloc — le mécanisme de découpage](#b-du-fichier-au-bloc--le-mécanisme-de-découpage)
  - [C. Du bloc au split, du split au mapper : combien de mappers ?](#c-du-bloc-au-split-du-split-au-mapper--combien-de-mappers-)
  - [D. Map](#d-map)
  - [E. Partition, shuffle et sort](#e-partition-shuffle-et-sort)
  - [F. Reduce](#f-reduce)
- [II. Temps 2 — Le contrat d'exécution](#ii-temps-2--le-contrat-dexécution)
  - [A. Un processus par tâche](#a-un-processus-par-tâche)
  - [B. L'en-tête n'existe que dans le premier split](#b-len-tête-nexiste-que-dans-le-premier-split)
  - [C. Tri lexicographique](#c-tri-lexicographique)
  - [D. Combien de reducers ?](#d-combien-de-reducers-)
  - [E. Le combiner ne sait pas moyenner](#e-le-combiner-ne-sait-pas-moyenner)
- [III. Temps 3 — Le geste](#iii-temps-3--le-geste)
  - [A. Hadoop Streaming : le contrat clé/valeur](#a-hadoop-streaming--le-contrat-clévaleur)
  - [B. Tester son code en local avant le cluster](#b-tester-son-code-en-local-avant-le-cluster)
  - [C. Lancer un job et lire les résultats](#c-lancer-un-job-et-lire-les-résultats)
  - [D. Suivre une application](#d-suivre-une-application)
  - [E. Quand un job échoue : les compteurs se lisent ailleurs](#e-quand-un-job-échoue--les-compteurs-se-lisent-ailleurs)
  - [F. `yarn logs` : la preuve dans les journaux](#f-yarn-logs--la-preuve-dans-les-journaux)
  - [G. La latence réelle d'un job](#g-la-latence-réelle-dun-job)
  - [H. Script d'automatisation](#h-script-dautomatisation)
- [IV. YARN](#iv-yarn)
  - [A. Présentation de YARN](#a-présentation-de-yarn)
  - [B. Fonctionnement de YARN](#b-fonctionnement-de-yarn)
  - [C. Allouer de la mémoire à MapReduce](#c-allouer-de-la-mémoire-à-mapreduce)
- [V. Exercices](#v-exercices)
  - [A. Map Reduce](#a-map-reduce)
  - [B. Exercices — dataset Spotify](#b-exercices--dataset-spotify)
  - [Exercice 1 — Total des streams par artiste](#exercice-1--total-des-streams-par-artiste)
  - [Exercice 2 — Moyenne des streams par année de sortie](#exercice-2--moyenne-des-streams-par-année-de-sortie)
  - [Exercice 3 — Nombre total de streams par année de sortie](#exercice-3--nombre-total-de-streams-par-année-de-sortie)
  - [Exercice 4 — Nombre moyen de playlists Spotify par année](#exercice-4--nombre-moyen-de-playlists-spotify-par-année)
  - [Exercice 5 — Statistiques visuelles des streams par année](#exercice-5--statistiques-visuelles-des-streams-par-année)
- [VI. Vérifiez vos acquis](#vi-vérifiez-vos-acquis)

## I. Temps 1 — Le modèle

### A. Le pipeline complet

MapReduce est le modèle de programmation du framework Hadoop : il permet d'analyser des volumes de données massifs en traitement **parallèle**. Les données sont découpées, traitées indépendamment sur plusieurs serveurs, puis les résultats partiels sont agrégés en un résultat consolidé.

Le traitement d'un job suit une chaîne **fixe**, où chaque étape est jouée par une pièce différente du framework :

![Le pipeline MapReduce : blocs, splits, map, partition, shuffle/sort, reduce](assets/mapreduce-pipeline.svg)

- **Bloc** : le fichier HDFS est découpé en blocs de **128 Mio** — le mécanisme de découpage est la section B ci-dessous. Notre CSV Spotify fait 155 593 octets : **un seul bloc**.
- **Split** : à chaque job, Hadoop découpe les blocs en *splits* — des « tranches de lecture » attribuées aux mappers. **Un split = une tâche map = un mapper.** Combien ? La section C répond, mesures à l'appui.
- **Map** : le mapper lit les lignes de son split (`stdin`), émet des paires intermédiaires `clé\tvaleur`.
- **Partition** : les paires sont routées vers les reducers — par défaut, « même clé → même reducer ».
- **Shuffle / sort** : le framework regroupe et **trie** les pairs par clé avant de les donner aux reducers.
- **Reduce** : le reducer agrège les valeurs de même clé, émet le résultat final.
- **Sortie** : un dossier HDFS contenant **un fichier par reducer** (`part-00000`, `part-00001`, …), plus un `_SUCCESS` vide si tout a bien fini.

![MR](assets/mr.png)

### B. Du fichier au bloc — le mécanisme de découpage

Le nombre de mappers d'un job se décide à partir de la façon dont HDFS range les fichiers. Le chapitre 03 a présenté le bloc comme l'unité que le NameNode connaît et que la réplication duplique ; on reprend ici ce qui compte pour le calcul.

- HDFS ne range pas un fichier entier quelque part : il le **découpe en blocs** de **128 Mio par défaut**, chaque bloc étant répliqué indépendamment. Notre CSV Spotify (155 593 octets) tient dans **un seul bloc**.
- La taille par défaut se configure dans `hdfs-site.xml` (`dfs.blocksize`) et **on peut y déroger fichier par fichier** au moment de l'envoi : `hdfs dfs -D dfs.blocksize=67108864 -put fichier.csv input/` (64 Mio pour celui-là).
- ⚠️ **Contrevérité classique :** la taille de bloc par défaut s'applique à tous les fichiers **sauf à ceux pour lesquels on a donné une valeur à la création** — elle n'est donc pas « forcément la même pour tous les fichiers ».
- Le compromis est simple : des blocs **gros** conviennent aux fichiers gros et peu nombreux ; des blocs **faibles** conviennent aux fichiers petits et très nombreux, et limitent le surcoût propre au travail par blocs.
- HDFS gère aussi la **compression à la volée** (codecs vus au chapitre 03) : elle économise l'espace et la bande passante, mais un fichier compressé **ne peut pas être découpé en splits** — un seul mapper le lirait donc en entier. C'est le prix, côté MapReduce, de cette économie de stockage.

### C. Du bloc au split, du split au mapper : combien de mappers ?

> **La règle à retenir de tout ce chapitre : le nombre de mappers se *lit*, il ne se devine pas.** La littérature dit « le nombre de splits = taille / taille de bloc ». Sur **notre** cluster, c'est faux : la vraie commande est lue dans le log du job, ligne `number of splits:`.

Ce qui a été **mesuré** sur le cluster du cours :

| Situation | Splits / mappers observés |
|---|---|
| une `-input` d'**un seul bloc** (155 593 o), sans option | **2 splits** — parce que `mapreduce.job.maps` vaut **2 par défaut** ici |
| même fichier, `-D mapreduce.job.maps=1` | **1 split** |
| même fichier, `-D mapreduce.job.maps=4` | **4 splits** (toujours le même bloc unique) |
| fichier de 1 095 829 o (1 bloc), `-D mapreduce.job.maps=8` | **8 splits**, `Map input records=6678` |

Deux enseignements :
1. Le nombre de splits dépend d'un **hint de configuration** (`mapreduce.job.maps`) autant que de la taille des données — pas d'une formule mentale.
2. « Un mapper = un split », mais **pas** forcément « un mapper = un bloc » : plusieurs mappers peuvent lire le même bloc.

Pour lire la vérité d'un job, après lancement :

```bash
hadoop jar … 2>&1 | grep "number of splits"
# 22/09/11 10:31:42 INFO mapreduce.JobSubmitter: number of splits:2
```

La commande complète de lancement est en III.C ; ici seul le filtre compte : `2>&1 | grep "number of splits"` met en évidence la ligne, le job se lance comme d'habitude.

### D. Map

- Le mapper reçoit les lignes de **son** split sur l'entrée standard, **une par tour de boucle**.
- Il émet sur la sortie standard des paires `clé\tvaleur` — **une paire par ligne**.
- **Un mapper = un processus `python3 mapper.py` lancé une fois.** Pas d'état conservé entre deux appels : tout ce qu'il doit « retenir », il le met dans la clé ou la valeur.


### E. Partition, shuffle et sort

Après la map, trois étapes intermédiaires préparent les données pour les reducers :

1. **Partition** : chaque paire intermédiaire est attribuée à un reducer. Par défaut, la partition est déterminée par la clé (hash) : **toutes les valeurs d'une même clé vont au même reducer**.
2. **Shuffle** : les paires voyagent du mapper vers le reducer qui les attend — et un reducer ne peut démarrer **tant qu'un mapper est encore actif**.
3. **Sort** : pour chaque reducer, les paires reçues sont **triées par clé**. C'est ce tri qui rend le reducer simple : il n'a qu'à accumuler les valeurs **consécutives** de même clé.

![mapreduce_unsort](assets/mapreduce_unsort.png) ![mapreduce_sort](assets/mapreduce_sort.png)

> Le tri est **lexicographique** (ordre des chaînes, pas des nombres) : `"10"` passe **avant** `"2"`. Conséquence concrète : les années sortent comme `1930, 1931, …, 2023` tant que les années ont 4 chiffres (alignement par hasard correct), mais `"2023" < "897"`. Et en HBase, on retrouvera le même tri — une clé `"1975"` arrive avant `"2023"` uniquement parce que `'1' < '2'`, pas parce que 1975 < 2023 (cf. séquence 06).

### F. Reduce

- Le reducer reçoit sur `stdin` des lignes `clé\tvaleur` **déjà groupées et triées par clé**.
- Il agrée les valeurs de chaque groupe et émet le résultat final, toujours en `clé\tvaleur`.
- **Un reducer = aussi un processus unique**, lancé entre 1 et N fois (une fois par *task* reduce). Comme le mapper, il ne garde rien entre deux exécutions.


## II. Temps 2 — Le contrat d'exécution

### A. Un processus par tâche

La conséquence la plus importante du modèle — et celle qu'on oublie le premier jour — : **chaque tâche (map comme reduce) est un processus isolé, lancé de zéro**. Un mapper ne connaît que ce qui passe sur son `stdin`, un reducer de même.

Trois corollaires concrets :

- **Pas de contention de ressources « intra-task »** : si votre code écrit un fichier sur le disque local, chaque mapper/Reducer l'écrit **à son propre emplacement** — le fichier de l'un écrase celui de l'autre.
- **Le graphe (EX 04, exercice 3) ne sort qu'à la fin du reducer** : le reducer lit **tout** son `stdin` avant de produire le PDF, puis `plt.savefig()` est appelé **après** la boucle — jamais pendant. Une sortie avant la fin produit un PDF vide.
- **« Retenir la clé précédente »** est le seul état toléré : on le porte dans des variables *locales* au processus (le pattern `current_key`), pas dans un fichier externe.

### B. L'en-tête n'existe que dans le premier split

Notre CSV a une ligne d'en-tête. Elle n'est **lue qu'une fois** — par le premier mapper à lire la première ligne du premier split. **Les autres mappers ne la voient jamais.**

> C'est pourquoi on écarte l'en-tête *par la valeur* (`if row[0] == 'track_name': continue`), **et jamais** par un `next(csv.reader(...))`. Mesuré sur le cluster : avec `next()`, le job **réussit**, `Map input records` est **identique** (954), et le résultat diffère d'**une paire** (1480 contre 1481) — impossible à distinguer avec les compteurs, dénombrable seulement en comparant à une valeur attendue.

### C. Tri lexicographique

Voir [Temps 1 — Partition, shuffle et sort](#partition-shuffle-et-sort) : le tri est sur les **clés sous forme de chaînes**. Vérifiez toujours, sur un petit échantillon, que vos clés trient dans l'ordre que votre reducer suppose.

### D. Combien de reducers ?

Par défaut : **1 reducer** — et donc **un seul fichier de sortie** : `part-00000`. Le `ls` du répertoire de sortie montre exactement un fichier de données plus le `_SUCCESS` :

```bash
hdfs dfs -ls output/wordcount
# Found 2 items
# -rw-r--r--   1 root supergroup  ... output/wordcount/_SUCCESS
# -rw-r--r--   1 root supergroup  ... output/wordcount/part-00000
```

Pour obtenir plusieurs fichiers (`part-00000`, `part-00001`, …), demandez plusieurs reducers — chaque reducer produira son propre fichier :

```bash
-D mapreduce.job.reduces=2
```

> **Pourquoi un seul fichier par défaut ?** Un seul reducer garantit une sortie **globalement triée** et un fichier unique, simple à relire. On augmente le nombre de reducers quand la sortie est volumineuse ou qu'on veut du parallélisme d'écriture — au prix de perdre le tri global et d'éclater le résultat en plusieurs fichiers.

![MR-word-count-2](assets/mr-word-count-2.png)

### E. Le combiner ne sait pas moyenner

Le **combiner** est un « mini-reducer » exécuté **sur le serveur du mapper**, avant le shuffle, pour alléger le trafic réseau : il agrée localement ce que le mapper vient d'émettre (souvent le même code que le reducer).

Un combiner suppose une propriété mathématique : les valeurs partielles doivent se combiner sans perdre d'information. La somme et le compte en bénéficient — **pas la moyenne** :

```
moyenne(10, 20, 30) = 20
combiner : groupe1 = moyenne(10, 20) = 15  ;  groupe2 = moyenne(30) = 30
moyenne(15, 30) = 22,5   ≠   20   ✗
```

Règle : **pour une moyenne, on ne combine jamais des moyennes.** On fait remonter **somme + compte**, et on divise **à la fin du reducer**. C'est exactement le pattern demandé par l'exercice « moyenne par année » de l'EX 04 — et la moyenne des moyennes est la signature d'un bug classique à repérer dans les rendus.

## III. Temps 3 — Le geste

### A. Hadoop Streaming : le contrat clé/valeur

MapReduce est écrit en Java, mais grâce à **Hadoop Streaming**, on écrit ses mappers et reducers dans **n'importe quel langage** (Python, Ruby, Bash…) : le jar streaming lit l'entrée sur `stdin` de vos programmes et collecte leur `stdout`. Le contrat tient en trois lignes :

- le **mapper** lit des lignes sur `stdin`, écrit sur `stdout` des paires `clé\tvaleur` (une par ligne) ;
- Hadoop fait la **partition** et le **shuffle/sort** entre les deux ;
- le **reducer** lit sur `stdin` les paires `clé\tvaleur` **déjà groupées et triées par clé**, écrit le résultat agrégé sur `stdout`.

**Structure minimale d'un mapper :**

```python
import sys

for line in sys.stdin:
    line = line.strip()
    # ... extraction de la clé et de la valeur ...
    print("%s\t%s" % (cle, valeur))
```

**Structure minimale d'un reducer** (agrégation des valeurs consécutives de même clé) :

```python
import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)
    if len(parts) < 2:
        continue
    key, value = parts[0], parts[1]
    try:
        value = int(value)
    except ValueError:
        continue

    if current_key == key:          # même clé : on accumule
        current_total += value
    else:
        if current_key:             # nouvelle clé : on émet la précédente
            print("%s\t%s" % (current_key, current_total))
        current_key = key
        current_total = value

if current_key:                     # ne pas oublier la dernière clé
    print("%s\t%s" % (current_key, current_total))
```

> **Pourquoi le `int()` est protégé ?** Une ligne mal formée (notre CSV en contient une : ligne 576, `streams` corrompu) fait planter un `int()` direct. En local le test le montre immédiatement ; sur le cluster, c'est **l'échec du job** avec un compteur trompeur (voir ci-dessous). Toujours tester en local d'abord.

Le jar à utiliser est fourni avec Hadoop : `hadoop-streaming-<version>.jar` (sur le cluster du cours, Hadoop 3.3.6), dans `$HADOOP_HOME/share/hadoop/tools/lib`.

### B. Tester son code en local avant le cluster

Simuler le pipeline MapReduce en local avec des pipes Unix est l'habitude la plus rentable du développement Hadoop :

```bash
# mapper seul : vérifier les paires émises
echo "bonjour bonsoir bonjour bonsoir" | python mapper.py

# mapper + reducer : sans le tri, le reducer échoue (clés non regroupées)
echo "bonjour bonsoir bonjour bonsoir" | python mapper.py | python reducer.py

# mapper + sort + reducer : simulation fidèle du shuffle
echo "bonjour bonsoir bonjour bonsoir" | python mapper.py | sort | python reducer.py
```

Le `sort` du shell **simule le shuffle** : sur le cluster, Hadoop regroupe et trie les clés avant le reducer. **Si votre code fonctionne avec `| sort |`, il fonctionnera sur le cluster** — et c'est aussi le moyen de voir les paires `clé\tvaleur` telles que le reducer les recevra.

> **Sous Windows** (avant de se connecter au master) : `type fichier.txt | python mapper.py | sort | python reducer.py`.


### C. Lancer un job et lire les résultats

Une fois le code validé en local, on soumet le job :

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-file mapper.py -mapper "python3 mapper.py" \
-file reducer.py -reducer "python3 reducer.py" \
-input input/Spotify_Most_Streamed_Songs.csv \
-output output/wordcount
```

- `-file` : envoie le script au cluster (il sera exécuté sur les nœuds) ;
- `-mapper` / `-reducer` : la commande exacte exécutée pour chaque tâche ;
- `-input` / `-output` : chemins **HDFS** (le répertoire de sortie **ne doit pas exister**).

> **Options dont on peut avoir besoin :** `-D mapreduce.job.maps=N` force le nombre de mappers (voir I.C) ; `-D mapreduce.job.reduces=N` force le nombre de reducers ; `-D mapreduce.job.name=…` donne un nom au job, affiché dans l'interface 8088. Pour lire le nombre de splits d'un job, ajouter `2>&1 | grep "number of splits"` à la commande.

> **ATTENTION :** le nom des fichiers doit être cohérent entre `-file` et `-mapper`/`-reducer` : si vous envoyez `mapper.py` avec `-file`, la commande doit référencer `mapper.py` (et pas un autre nom).

Le résultat est un répertoire HDFS, un fichier par reducer :

```bash
hdfs dfs -ls output/wordcount     # part-00000, part-00001, _SUCCESS
hdfs dfs -cat output/wordcount/part-00000   # lire le résultat complet
# (tail ne montre que le dernier Ko : pour un petit résultat, préférez -cat)
```

![MapReduce response](assets/mr-response.png)

### D. Suivre une application

Le ResourceManager de YARN expose une interface web : **<http://localhost:8088>**. On y suit l'état (RUNNING, SUCCEEDED, FAILED), la progression, et — réflexe n°1 quand un job échoue — les liens vers les logs détaillés.

Le comportement des nœuds esclaves est visible sur **<http://localhost:8041>** (slave1) et **<http://localhost:8042>** (slave2) : mémoire, conteneurs actifs, tâches en cours.

![MapReduce Process](assets/mr-process.jpeg) ![MapReduce Stats](assets/mr-stats.jpeg)

### E. Quand un job échoue : les compteurs se lisent ailleurs

Un job en échec, c'est un job dont on a le *résultat* — pas forcément le *diagnostic*. Deux cas mesurés sur le cluster :

**1. `int(row[8])` non protégé** (ligne 576 corrompue) :

```
PipeMapRed.waitOutputThreads(): subprocess failed with code 1
```

Le job échoue après plusieurs tentatives. Les compteurs affichent `Map input records=473` — **le seul task survivant**, pas les 954. **Ne lisez jamais les compteurs d'un job en échec comme ceux d'un job réussi** : un compteur n'est fiable que si l'on sait *quel* task l'a monté.

**2. `next(csv.reader(...))` pour sauter l'en-tête** : le job **réussit**, `Map input records` est **identique** (954), mais il manque une paire (1480 contre 1481). Les compteurs ne le montrent pas — c'est en comparant à **une valeur attendue** (par exemple le `COUNT(*)` de Hive, ou le total calculé en Python) qu'on le détecte.

Il y a donc **deux sortes d'échec** : l'échec *visible* (yarn logs, comme le `subprocess failed with code 1`) et l'échec *silencieux* (job vert, résultat faux d'une paire). Le second exige de disposer d'une **valeur de contrôle** écrite avant le job.

### F. `yarn logs` : la preuve dans les journaux

Quand l'UI 8088 ne suffit plus, on va chercher la preuve dans les journaux :

```bash
yarn logs -applicationId application_1690000000000_0001 2>&1 | grep -n "PipeMapRed\|stderr\|Exception" | head
```

- `application_…` : l'identifiant affiché dans l'UI 8088 (et dans la sortie du launcher, après `INFO mapreduce.JobSubmitter`) — le prenez **avant** l'échec ou dans l'historique.
- L'option `-containerId` cible un conteneur précis (ex. `container_e1_1690000000000_0001_01_000002`) ; sans elle, `yarn logs` affiche les journaux de **toutes** les tâches. Pour lister les conteneurs avec leur nœud : `-show_application_log_info`.

> Cette commande manquait au cursus alors que le « réflexe n°1 » y est affirmé : **c'est *le* moyen de voir le `stderr` d'un mapper qui a planté** (exception Python, code de sortie, ligne en cause).

### G. La latence réelle d'un job

Dimensionnez votre journée avec la mesure, pas avec le mythe d'un MapReduce instantané. Sur le cluster du cours, même un « petit » job a un prix fixe : **35 s à 69 s pour un `COUNT(*)` sur un CSV de 154 ko** — allocation des conteneurs, upload des scripts, lancement des JVM, écriture sur HDFS.

Quand un exercice annonce « je lance un job », prévoir : chaque lancement = 1 à 2 minutes de cluster, à multiplier par le nombre d'essais. C'est la raison n°1 de **tester en local d'abord** (section précédente) : chaque aller-retour cluster en échec coûte ce temps-là.

### H. Script d'automatisation

L'enchaînement « démarrer les services → créer les répertoires HDFS → copier les données → soumettre le job → lire le résultat » est répétitif. Un script shell automatise tout. Exemple complet fourni dans [`annexes/utils/0-job.sh`](annexes/utils/0-job.sh) :

```bash
#!/bin/bash

INPUT_FILE="Spotify_Most_Streamed_Songs.csv"
OUTPUT_FILE="exemple"

./start-dfs.sh                     # NameNode & DataNodes (./ : /home hors PATH)
./start-yarn.sh                    # ResourceManager & NodeManagers
# pas de HBase/Thrift ici : ce job écrit sur stdout (voyez utils/5 pour HBase)

hdfs dfs -mkdir -p input
hdfs dfs -put -f $INPUT_FILE input
hdfs dfs -rm -r -f output/$OUTPUT_FILE    # le répertoire de sortie ne doit pas exister

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-file mapper.py -mapper "python3 mapper.py" \
-file reducer.py -reducer "python3 reducer.py" \
-input input/$INPUT_FILE \
-output output/$OUTPUT_FILE

hdfs dfs -cat output/$OUTPUT_FILE/part-00000
```

Adaptez les noms de fichiers d'entrée/sortie et les services nécessaires à votre job (pas besoin de HBase si votre reducer écrit sur stdout).

## IV. YARN

### A. Présentation de YARN

- **YARN** = « **Y**et **A**nother **R**esource **N**egotiator ». C'est le gestionnaire de ressources de Hadoop, devenu sous-projet Apache avec Hadoop 2.0 (2012-2013).
- Avant YARN, Hadoop ne pouvait exécuter que des applications MapReduce. En découplant la **gestion des ressources** de l'**exécution des traitements**, YARN permet à Hadoop d'héberger d'autres types d'applications (Spark, Hive, Tez…) sur le même cluster.

### B. Fonctionnement de YARN

Trois rôles :

- **ResourceManager** (RM) — sur le master : alloue les ressources, planifie les applications.
- **NodeManager** (NM) — sur chaque nœud : surveille les conteneurs, remonte l'état au RM.
- **ApplicationMaster** (AM) — un par application : négocie les conteneurs auprès du RM, orchestre les tâches de *son* application.

![RessourceManger](assets/ressourcemanger.jpeg)

YARN propose plusieurs politiques de planification : **FIFO Scheduler** (premier arrivé, premier servi), **Fair Scheduler** (les applications se partagent la capacité équitablement), **Capacity Scheduler** (la défaut : quotas par file d'attente).

![Scheduling-policies](assets/scheduling-policies.jpeg) ![Hadoop Scheduler](assets/scheduler.png) ![Hadoop Fair Scheduler](assets/fair-scheduler.jpeg)

La stratégie est configurée dans `etc/hadoop/yarn-site.xml` (propriété `yarn.resourcemanager.scheduler.class`).

### C. Allouer de la mémoire à MapReduce

Sur le cluster du cours, les conteneurs sont petits — et c'est un choix, pas un oubli : `yarn.nodemanager.resource.memory-mb=256` (mémoire gérée par YARN par NodeManager) et `mapreduce.map.memory.mb=192` (mémoire demandée par chaque tâche map). N'attendez pas des mappers « géants » : avec 192 Mio par tâche, un mapper Python tient, mais le parallélisme (et la taille des données qu'une tâche peut tenir en mémoire) est forcément modeste. Pour un job qui en a besoin, on augmente le nombre de tâches plutôt que la mémoire de chacune.

![yarn-config](assets/yarn-config.png)

## V. Exercices

> Batterie de données de référence (mesurée en session cluster du 11/09) : tout ce qui est en « Résultat attendu » ici a été reproduit au chiffre près — par un job réel, puis recoupé indépendamment en Python et en SQL (Hive).

### A. Map Reduce

#### 1. Présentation

Un Job Map-Reduce se compose principalement de deux types de programmes :
- **Mappers :** permettent d'extraire les données nécessaires sous forme de clef/valeur, pour pouvoir ensuite les trier selon la clef
- **Reducers :** prennent un ensemble de données triées selon leur clef, et effectuent le traitement nécessaire sur ces données (somme, moyenne, total...)

#### 2. Wordcount

Nous allons tester un programme **MapReduce** grâce à un exemple très simple, le **WordCount**, l'équivalent du **Hello World!** pour les applications de traitement de données. Le **WordCount permet de calculer le nombre de mots dans un fichier donné**, en décomposant le calcul en deux étapes :
- L'étape de **Mapping**, qui permet de **découper le texte** en mots et de délivrer en sortie un flux textuel, où chaque ligne contient le mot trouvé, suivi de la valeur 1 (pour dire que le mot a été trouvé une fois)
- L'étape de **Reducing**, qui permet de faire la somme des 1 pour chaque mot, pour **trouver le nombre total d'occurrences** de ce mot dans le texte.

##### 1. Création du projet en Python

> *vi mapper.py*

```python
import sys

# Lecture de l'entrée standard, une ligne à la fois
for line in sys.stdin:
    words: list[str] = line.strip().split(" ")  # séparateur 'espace' à changer
    for word in words:
        if word:  # mot non vide
            print(f"{word.lower()}\t1")  # clé\valeur
```

> 🔎 Le `print` émet **une ligne = une paire** `mot\t1` — le contrat clé/valeur d'Hadoop Streaming est un flux texte, rien de plus.

> *vi reducer.py*

```python
import sys

current_word: str | None = None
current_count: int = 0

for line in sys.stdin:
    parts = line.strip().split("\t", 1)
    if len(parts) < 2:  # ligne mal formée : on l'ignore
        continue
    word, count_str = parts

    try:
        count = int(count_str)
    except ValueError:
        continue  # valeur non numérique : on l'ignore

    # Les clés arrivent groupées et triées (le shuffle s'en charge)
    if current_word == word:
        current_count += count
    else:
        if current_word is not None:  # ne pas imprimer la première ligne
            print(f"{current_word}\t{current_count}")
        current_count = count
        current_word = word

if current_word is not None:  # le dernier mot comptabilisé
    print(f"{current_word}\t{current_count}")
```

##### 2. Test local avant le cluster

Avant de lancer un job sur le cluster, **testez toujours votre code en local** : plus rapide, pas de ressources consommées, erreurs détectées immédiatement.

Mapper, Sort et Reducer

`echo "bonjour bonsoir bonjour bonsoir" | python mapper.py | sort | python reducer.py`

![mapreduce_sort](assets/mapreduce_sort.png)

**Note ** N'oubliez pas qu'on doit **sort** entre le map et reduce car Hadoop envoie les données triées au reducer (l'étape de *shuffle* regroupe et trie les clés pour vous).

##### 3. Utilisation de notre Map-Reduce dans Hadoop

- Lancer les services Hadoop (dans le master) : **./start-dfs.sh** puis **./start-yarn.sh** (ou **./start-all.sh**)
- Vérifier si le fichier **hadoop-streaming-\*.jar** existe bien dans le dossier (sur le cluster du cours, Hadoop 3.3.6) :
`ls $HADOOP_HOME/share/hadoop/tools/lib`
- Vérifier que le CSV Spotify est dans HDFS (chargé à l'EX 03) :

  `hdfs dfs -ls input`

  _Si le listing est vide, refaites le chargement : `hdfs dfs -mkdir -p input` puis `hdfs dfs -put Spotify_Most_Streamed_Songs.csv input`._
- Créer le répertoire de sortie :

  `hdfs dfs -mkdir output`

  _Crée le dossier `/user/root/output`, le répertoire de sortie du job. Il ne doit pas exister au lancement du job, sinon Hadoop refuse (le script `annexes/utils/0-job.sh` le supprime avant chaque run)._
- Lancer le mapper et le reducer sur Hadoop avec le CSV Spotify :
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-file mapper.py -mapper "python3 mapper.py" \
-file reducer.py -reducer "python3 reducer.py" \
-input input/Spotify_Most_Streamed_Songs.csv \
-output output/wordcount
```

> **ATTENTION ** Nom mapper & reducer doivent être pareil entre le `-file`, le `-mapper` et le `-reducer`.

- Afficher le résultat stocké sur HDFS : `hadoop fs -tail output/wordcount/part-00000`

##### 4. Vérification par les outils Web d'Hadoop

Il vous est possible de monitorer vos Jobs Map Reduce, en allant à la page : **<http://localhost:8088>**. Vous trouverez votre Job dans la liste des applications comme suit :

![MapReduce Process](assets/mr-process.jpeg)

Il est également possible de voir le comportement des noeuds esclaves, en allant à l'adresse : **<http://localhost:8041>** pour **slave1**, et **<http://localhost:8042>** pour **slave2**. Vous obtiendrez ce qui suit :

![MapReduce Stats](assets/mr-stats.jpeg)

### B. Exercices — dataset Spotify

Les exercices suivants approfondissent MapReduce avec des **données réelles** issues de Spotify (nombre de streams, playlists, artistes…) — le même fichier `data/Spotify_Most_Streamed_Songs.csv` que partout dans le cours.

Pour réaliser ces questions, vous utiliserez le comportement MapReduce de Hadoop grâce à deux fichiers :
- le **mapper**, pour filtrer les données
- le **reducer**, pour agréger et exporter les données

**Aides fournies** : des squelettes et scripts de référence sont disponibles dans [`annexes/utils/`](annexes/utils/) :
- `1-structure-mapper.py` / `2-structure-reducer.py` — structure minimale d'un mapper/reducer
- `3-mapper-txt.py` — mapper sur fichier texte
- `4-mapper-csv-reader.py` — mapper lisant un CSV (gère les virgules quotées)
- `0-job.sh` — script d'automatisation d'un job complet (démarrage, put, lancement, résultat)

### Exercice 1 — Total des streams par artiste

- Écrire un Job **Map Reduce** permettant, à partir du **CSV Spotify**, de déterminer le **total des streams par artiste**.

Repérez d'abord les colonnes dans le fichier (`head -1` en local) — le CSV a 25 champs, en voici les repères utiles pour cet exercice :

| index | 0 | 1 | 2 | ... | 8 |
| --- | --- | --- | --- | --- | --- |
| champ | `track_name` | `artist(s)_name` | `artist_count` | ... | `streams` |

**Mapper** : extraire le champ artiste (index 1) et `streams` (index 8).

> ⚠️ **`csv.reader`, pas `split(',')`.** Le champ artiste contient des virgules **dans les champs quotés** (`"Latto, Jung Kook"`). Un `split(',')` naïf décale toutes les colonnes qui suivent. Passez par `csv.reader(sys.stdin, delimiter=',')`, qui respecte les guillemets CSV.

**Reducer** : agréger la somme des streams par artiste (même structure que le reducer du WordCount).

**Résultat attendu** (valeurs mesurées — convention : la colonne artiste **telle quelle**, un duo = une clé unique) : **644 artistes**, dont en tête :

```
The Weeknd	14185552870
Taylor Swift	14053658300
Ed Sheeran	13908947204
Harry Styles	11608645649
Bad Bunny	9997799607
Olivia Rodrigo	7442148916
```

> 🤔 **Question de contrôle — les conventions.** Éclatez les duos (chaque membre se voit créditer du total du titre) et le classement change de tête : **The Weeknd 23 929 760 757**, **Bad Bunny 23 813 527 270**, Ed Sheeran 15 316 587 718, Taylor Swift 14 630 378 183. Deux réponses justes à la même question — **la vôtre dépend du mapper, et c'est vous qui l'écrivez**. Indiquez votre convention dans votre rapport. La séquence 05 (Hive) refait ce calcul en SQL : la validation croisée n'a de sens que si les deux côtés appliquent la même convention.

<details>
<summary>Voir la correction</summary>

Le CSV Spotify a 25 champs ; avec `csv.reader`, l'artiste est `row[1]` et `streams` `row[8]`. Le champ artiste contient des virgules **dans les champs quotés** (`"Latto, Jung Kook"`) : le `csv.reader` est donc non négociable.

**mapper.py**

```python
import sys
import csv

for row in csv.reader(sys.stdin, delimiter=','):
    if len(row) < 9 or row[0] == 'track_name':   # sauter l'en-tête
        continue
    artist = row[1]
    try:
        streams = int(float(row[8]))              # ligne 576 : streams corrompu -> écartée
    except ValueError:
        continue
    print("%s\t%s" % (artist, streams))
```

> ⚠️ **Ligne 576 du CSV** : la ligne de *Love Grows (Where My Rosemary Goes)* a un champ `streams` corrompu (le contenu de 9 colonnes s'y est fondu). Sans le `try/except`, `int(row[8])` lève une exception **sur le cluster** et le job échoue (`PipeMapRed.waitOutputThreads(): subprocess failed with code 1`, compteur `Map input records=473` : seul le task survivant compte — ne lisez jamais les compteurs d'un job en échec comme ceux d'un job réussi).

**reducer.py**

```python
import sys

current_artist = None
total = 0

for line in sys.stdin:
    artist, streams = line.strip().split('\t', 1)
    try:
        streams = int(streams)
    except ValueError:
        continue
    if current_artist == artist:
        total += streams
    else:
        if current_artist:
            print("%s\t%d" % (current_artist, total))
        current_artist = artist
        total = streams

if current_artist:
    print("%s\t%d" % (current_artist, total))
```

**Sortie attendue** (mesurée) — convention « la colonne artiste telle quelle » (un duo = une clé unique) :

```
The Weeknd	14185552870
Taylor Swift	14053658300
Ed Sheeran	13908947204
Harry Styles	11608645649
Bad Bunny	9997799607
Olivia Rodrigo	7442148916
```

- **644 artistes** (version non éclatée) ; somme de la colonne `streams` (ligne 576 exclue) : **489 458 828 542**.
- **Version « duos éclatés »** (chaque membre credité du total du titre) — c'est la question de contrôle de l'énoncé : **698 artistes**, dont The Weeknd **23 929 760 757**, Bad Bunny **23 813 527 270**, Ed Sheeran 15 316 587 718, Taylor Swift 14 630 378 183, Harry Styles 11 608 645 649, Eminem 10 193 727 260. La tête change par *nombre de titres* aussi : Bad Bunny 40, Taylor Swift 38, The Weeknd 37.

> La convention choisie par l'apprenant doit être écrite dans son rapport : c'est elle que la validation croisée avec Hive (EX 05) suppose — `GROUP BY artist_name` en Hive donne exactement la version non éclatée ci-dessus, le `part-00000` du MR éclaté ne doit **pas** lui être comparé tel quel.

</details>

### Exercice 2 — Moyenne des streams par année de sortie

- Écrire un Job **Map Reduce** permettant de déterminer la **moyenne des streams par année de sortie**.

**Mapper** : extraire `released_year` (index 3) et `streams` (index 8).

**Reducer** : accumuler **la somme et le nombre d'occurrences** par année, et émettre la moyenne en fin de groupe — même mécanique que la somme de l'exercice 1, avec deux accumulateurs au lieu d'un. Format de sortie : `année\tmoyenne`.

**Résultat attendu** : une ligne par année de sortie. Vérification de plausibilité : les années s'étendent de **1930 à 2023** (50 années), la plupart des moyennes autour de quelques centaines de millions de streams.

> ⚠️ **Le combiner ne sait pas moyenner.** Une moyenne ne se combine pas : la moyenne des moyennes de deux groupes n'est pas la moyenne globale. Sur ce job, n'utilisez **pas** de combiner — ou alors un combiner « somme + compte » dont le reducer redescend la vraie moyenne. Le cours (séquence 04) développe ce piège.

> ✨ **Lien avec la séquence 05** : l'EX 05 (Hive) demande exactement ce calcul en SQL (`AVG(streams) … GROUP BY released_year`). Vous pourrez confronter vos deux chiffres ligne à ligne.

<details>
<summary>Voir la correction</summary>

Même mapper, avec `released_year = row[3]` à la place de l'artiste. Le reducer accumule **somme et compte** (et non la moyenne au fil de l'eau), puis émet la moyenne à la fin de chaque groupe :

**reducer.py**

```python
import sys

current_year = None
total = 0
count = 0

for line in sys.stdin:
    year, streams = line.strip().split('\t', 1)
    try:
        streams = int(streams)
    except ValueError:
        continue
    if current_year == year:
        total += streams
        count += 1
    else:
        if current_year:
            print("%s\t%.1f" % (current_year, total / count))
        current_year = year
        total = streams
        count = 1

if current_year:
    print("%s\t%.1f" % (current_year, total / count))
```

- Plage attendue : **1930 → 2023** (50 années). Ordres de grandeur : quelques centaines de millions, **pas** des milliards — si vous tombez sur des valeurs comparables aux sommes de l'exercice 1, la moyenne est mal calculée.
- **Piège** : le combiner ne sait pas moyenner (moyenne des moyennes ≠ moyenne globale). Somme + compte, jamais la moyenne, en combiner.
- **Corollaire d'exécution** : un reducer est un *processus* lancé entre 1 et N fois. La moyenne se calcule en fin de groupe sur les accumulateurs, jamais après coup sur la sortie.

**Validation croisée EX 05** : `SELECT released_year, AVG(streams) … GROUP BY released_year` doit donner les mêmes valeurs ligne à ligne (ligne 576 passée à `NULL` de part et d'autre).

</details>

### Exercice 3 — Nombre total de streams par année de sortie

**Objectif** : Calculer le nombre total de streams par année.

- **Mapper** : Lire le fichier CSV, extraire l'année de sortie (`released_year`) et le nombre de streams (`streams`).
- **Reducer** : Agréger le nombre total de streams par année.

**Résultat attendu** : une ligne par année `année\ttotal_streams`, triée par année croissante. Extrait (valeurs **mesurées** — deux recalculs indépendants sur le CSV réel) :

```
1975	4206105352
1984	3471625366
...
2023	25808484104
```

> ⚠️ Le nombre de streams est un très grand entier (le maximum mesuré dépasse 3,7 milliards) — attention aux conversions de type et aux valeurs aberrantes de certaines lignes du CSV. La ligne 576 a un champ `streams` corrompu : votre `int()` doit être protégé, sinon le job échoue.

<details>
<summary>Voir la correction</summary>

**mapper.py**

```python
import sys
import csv

for row in csv.reader(sys.stdin, delimiter=','):
    if len(row) < 9 or row[0] == 'track_name':   # sauter l'en-tête
        continue
    released_year, streams = row[3], row[8]
    try:
        streams = int(float(streams))           # certaines lignes sont sales
    except ValueError:
        continue
    if released_year.isdigit():
        print("%s\t%s" % (released_year, streams))
```

> ⚠️ Le CSV Spotify contient des virgules **dans les champs texte** (noms de titres entre guillemets) : passer par `csv.reader` plutôt qu'un simple `split(',')` évite de décaler les colonnes. Les index exacts (`row[3]`, `row[8]`) dépendent de la version du dataset — vérifiez avec `head -1` du CSV.

**reducer.py** : identique au reducer « somme » (somme des streams par clé), en remplaçant la protection d'en-tête par le filtre numérique décrit plus haut.

**Sortie attendue** (extrait — valeurs **mesurées**, à reproduire au chiffre près) :

```
1975	4206105352
1984	3471625366
...
2023	25808484104
```

Total de la colonne `streams` sur les 953 titres (ligne 576 au `streams` corrompu exclue) : **489 458 828 542** streams.

> Ces trois lignes sont la **clé de voûte de la validation croisée MR ↔ Hive** de l'EX 05 : le `GROUP BY released_year` de Hive doit rendre exactement ces valeurs. Elles ont été recalculées deux fois (Python avec quoting CSV, puis `SUM()` dans Hive) et concordent.
>
> ⚠️ Avec un mapper qui fait `int(row[8])` sans protection, le job **échoue** sur la ligne 576 du CSV (`streams` corrompu, année 1970) : `PipeMapRed.waitOutputThreads(): subprocess failed with code 1`, et les compteurs affichent `Map input records=473` — le task survivant seul, pas la totalité. Ne lisez jamais les compteurs d'un job en échec comme ceux d'un job réussi.

</details>

### Exercice 4 — Nombre moyen de playlists Spotify par année

**Objectif** : Calculer la moyenne du nombre de playlists Spotify contenant des chansons, regroupées par année de sortie.

- **Mapper** : Extraire l'année de sortie (`released_year`) et le nombre de playlists Spotify (`in_spotify_playlists`).
- **Reducer** : Calculer la moyenne du nombre de playlists par année.

**Résultat attendu** : une ligne par année `année\tmoyenne_playlists`. Contrairement à l'exercice 3, votre reducer doit accumuler **à la fois** la somme et le nombre d'occurrences pour calculer la moyenne — même structure que l'exercice 2, reformatée sur une autre colonne.

<details>
<summary>Voir la correction</summary>

Même mapper avec `in_spotify_playlists` à la place de `streams`. Reducer « moyenne » : cf. exercice 2 de l'EX 04 (somme + compte, moyenne en fin de groupe). Vérification rapide : les moyennes doivent rester dans un ordre de grandeur plausible (quelques centaines de playlists), pas des milliards.

</details>

### Exercice 5 — Statistiques visuelles des streams par année

**Objectif** : Créer un graphique des streams par année, exporté en PDF.

- **Mapper** : Comme dans l'exercice 3, extraire l'année de sortie (`released_year`) et le nombre de streams (`streams`).
- **Reducer** : Agréger le nombre total de streams par année, créer un graphique matplotlib (pie) pour visualiser les tendances et enfin, l'exporter en PDF.

**Résultat attendu** : un fichier PDF contenant le camembert des streams par année, à côté de la sortie standard agrégée. Référence : [`annexes/utils/6-reducer-matplotlib-pdf.py`](annexes/utils/6-reducer-matplotlib-pdf.py).

<details>
<summary>Voir la correction</summary>

Reducer : agrégation identique à l'exercice 3, puis génération matplotlib **à la fin du stdin** (toutes les clés vues) :

```python
import sys
import matplotlib
matplotlib.use('Agg')            # pas de display sur le cluster
import matplotlib.pyplot as plt

years, totals = [], []
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
            years.append(int(current_year)); totals.append(total)
        current_year, total = year, streams

if current_year:
    years.append(int(current_year)); totals.append(total)

plt.figure(figsize=(8, 8))
plt.pie(totals, labels=years, autopct='%1.1f%%')
plt.title('Streams par année de sortie')
plt.savefig('streams_par_annee.pdf')
```

> **Piège** : `plt.savefig` doit être appelé **après** la boucle de lecture stdin, sinon le PDF est vide. Référence complète : [`annexes/utils/6-reducer-matplotlib-pdf.py`](annexes/utils/6-reducer-matplotlib-pdf.py).

</details>

## VI. Vérifiez vos acquis

*Formatif, jamais noté. Répondez sans relire le chapitre. Les réponses se déplient juste après le séparateur : ce qui compte est de repérer où vous vacillez, pas de faire 5 sur 5.*

**1.** Comment connaître avec certitude le nombre de mappers d'un job lancé sur le cluster du cours ?

- A. En divisant la taille du fichier par la taille de bloc de 128 Mio, la formule de la littérature
- B. En comptant les blocs HDFS du fichier d'entrée avec `hdfs dfs -ls -R input`
- C. En lisant la ligne `number of splits:` dans le log du job, à son lancement
- D. En regardant combien de fichiers `part-00000` le job a produits dans le répertoire de sortie

**2.** Pourquoi ne pas mettre de combiner sur le job « moyenne des streams par année » ?

- A. Parce que le combiner s'exécute sur le nœud du reducer, après le shuffle : il ne peut rien alléger
- B. Parce que le combiner peut être rejoué après le tri, ce qui ferait compter des valeurs deux fois
- C. Parce que le combiner écarte les lignes mal formées et ferait disparaître la ligne 576 du CSV
- D. Parce que la moyenne de deux moyennes partielles ne rend pas la moyenne globale du groupe

**3.** Pourquoi l'en-tête du CSV doit-il être écarté par la valeur (`if row[0] == 'track_name'`) et non par un `next()` ?

- A. Parce que `next()` lève une exception sur la ligne 576 du CSV, celle au `streams` corrompu, et fait échouer le job
- B. Parce que l'en-tête n'est lu que par le premier mapper : le job réussit et le résultat ne diffère que d'une paire
- C. Parce que `next()` lit deux lignes au lieu d'une sur chaque split, ce qui fausse `Map input records`
- D. Parce que l'en-tête arrive toujours en dernier dans le split, après la totalité des lignes de données

**4.** Dans YARN (Yet Another Resource Negotiator), qui négocie les conteneurs auprès du ResourceManager pour une application donnée ?

- A. L'ApplicationMaster, il y en a un pour chaque application
- B. Le NodeManager, présent sur chacun des nœuds du cluster
- C. Le Capacity Scheduler, la politique de planification par défaut
- D. Le mapper lui-même, qui demande la mémoire dont il a besoin

**5.** *Question ouverte (2-3 lignes).* Un job échoue avec `subprocess failed with code 1` et les compteurs affichent `Map input records=473`, alors que le même job mené à bout affiche 954. Que s'est-il passé, et pourquoi ce compteur ne montre-t-il pas la totalité ?

---

<details>
<summary>Vérifier ses réponses</summary>

### Réponses

**1. C.** Le nombre de mappers se lit dans le log du job, ligne `number of splits:` — sur notre cluster, un fichier d'un seul bloc donne 2 splits sans option, parce que `mapreduce.job.maps` vaut 2 par défaut.
**Pourquoi :** la taille du bloc ne fixe pas à elle seule le nombre de splits, un hint de configuration pèse autant qu'elle — la formule « taille / bloc » de la littérature est donc fausse ici.

**2. D.** Une moyenne ne se combine pas : moyenne(10, 20, 30) = 20, mais moyenne(15, 30) = 22,5.
**Pourquoi :** le combiner agrège localement avant le shuffle, et des valeurs partielles ne se combinent sans perte que pour une somme ou un compte — pour une moyenne, on fait remonter somme + compte et on divise à la fin du reducer.

**3. B.** L'en-tête n'existe que dans le premier split : avec `next()`, le job réussit, `Map input records` reste à 954 et le résultat diffère d'une seule paire (1480 contre 1481).
**Pourquoi :** chaque mapper est un processus isolé qui ne lit que son split, donc `next()` ne filtre l'en-tête que chez le premier — les compteurs ne le révèlent pas, seule une valeur de contrôle attendue le montre.

**4. A.** L'ApplicationMaster est dédié à une application : il négocie les conteneurs auprès du ResourceManager et orchestre les tâches de son application.
**Pourquoi :** le ResourceManager alloue et planifie à l'échelle du cluster, le NodeManager surveille les conteneurs sur son nœud, et le scheduler n'est qu'une politique — aucun des trois ne pilote une application particulière.

**5.** Attendu : le mapper a planté sur un `int()` non protégé (ligne 576 du CSV, champ `streams` corrompu), Hadoop a relancé la tâche sans plus de succès, et le job est tombé. Le 473 est le compte du seul task survivant, pas la totalité des lignes.
**Pourquoi :** un compteur n'est fiable que si l'on sait quel task l'a monté — on ne lit jamais les compteurs d'un job en échec comme ceux d'un job réussi, et la preuve se cherche dans `yarn logs -applicationId …`.

</details>

---

[← Chapitre précédent](03-hdfs.md) · [Sommaire](Readme.md) · [Chapitre suivant →](05-hive.md)
