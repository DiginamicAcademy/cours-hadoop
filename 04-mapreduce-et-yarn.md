[← Chapitre précédent](03-hdfs.md) · [Sommaire](Readme.md) · [Chapitre suivant →](05-hive.md)

![Diginamic Logo](assets/diginamic.png) ![Hadoop Logo](assets/hadoop.png)

# 04 - MapReduce et YARN

> **Objectifs** : expliquer le pipeline MapReduce et distinguer **bloc HDFS**, **split MapReduce** et **conteneur YARN** · expliquer le rôle de YARN (ResourceManager, NodeManager, ApplicationMaster, schedulers) · écrire un mapper et un reducer minimaux, les tester en local et lancer un job Hadoop Streaming · diagnostiquer les erreurs du premier lancement et lire les logs YARN · automatiser un job reproductible avec un script shell.
> **La vérité vient de la mesure** : les chiffres de ce chapitre (nombre de splits, latences, compteurs, mémoire) ont été reproduits sur le cluster du cours (Hadoop 3.3.6). Quand une règle de la littérature ne colle pas à notre cluster, c'est la mesure qui fait foi.
> **Mise en pratique** : [Exercices](#iv-exercices)

## Sommaire

- [I. Théorie — comprendre avant d'exécuter](#i-théorie--comprendre-avant-dexécuter)
  - [A. Le problème que résout MapReduce](#a-le-problème-que-résout-mapreduce)
  - [B. WordCount sans code : le principe](#b-wordcount-sans-code--le-principe)
  - [C. Du fichier au mapper : qui découpe quoi ?](#c-du-fichier-au-mapper--qui-découpe-quoi-)
  - [D. YARN : le gestionnaire de ressources](#d-yarn--le-gestionnaire-de-ressources)
  - [E. Entre le map et le reduce : partition, shuffle, tri](#e-entre-le-map-et-le-reduce--partition-shuffle-tri)
  - [F. Ce que le modèle impose au code](#f-ce-que-le-modèle-impose-au-code)
- [II. Application simple — le premier job à la main](#ii-application-simple--le-premier-job-à-la-main)
  - [A. Le contrat Hadoop Streaming](#a-le-contrat-hadoop-streaming)
  - [B. Structure minimale du mapper](#b-structure-minimale-du-mapper)
  - [C. Structure minimale du reducer](#c-structure-minimale-du-reducer)
  - [D. Tester en local avant le cluster](#d-tester-en-local-avant-le-cluster)
  - [E. La commande `hadoop jar` la plus simple](#e-la-commande-hadoop-jar-la-plus-simple)
  - [F. Les 3 avertissements du premier lancement](#f-les-3-avertissements-du-premier-lancement)
  - [G. Quand ça ne marche pas : lire YARN](#g-quand-ça-ne-marche-pas--lire-yarn)
- [III. Simplifier les exécutions — le job reproductible](#iii-simplifier-les-exécutions--le-job-reproductible)
  - [A. Pourquoi automatiser (et ce qui reste manuel)](#a-pourquoi-automatiser-et-ce-qui-reste-manuel)
  - [B. La structure fournie](#b-la-structure-fournie)
  - [C. Le script minimal, commenté ligne par ligne](#c-le-script-minimal-commenté-ligne-par-ligne)
  - [D. La version outillée fournie](#d-la-version-outillée-fournie)
  - [E. Les variantes fournies](#e-les-variantes-fournies)
  - [F. Vérifier, rejouer, et ne pas confondre script et orchestrateur](#f-vérifier-rejouer-et-ne-pas-confondre-script-et-orchestrateur)
- [IV. Exercices](#iv-exercices)
  - [A. Dataset et aides](#a-dataset-et-aides)
  - [Exercice 1 — Total des streams par artiste](#exercice-1--total-des-streams-par-artiste)
  - [Exercice 2 — Moyenne des streams par année de sortie](#exercice-2--moyenne-des-streams-par-année-de-sortie)
  - [Exercice 3 — Nombre total de streams par année de sortie](#exercice-3--nombre-total-de-streams-par-année-de-sortie)
  - [Exercice 4 — Nombre moyen de playlists Spotify par année](#exercice-4--nombre-moyen-de-playlists-spotify-par-année)
  - [Exercice 5 — Statistiques visuelles des streams par année](#exercice-5--statistiques-visuelles-des-streams-par-année)
  - [Défi — automatiser avec 0-job.sh](#défi--automatiser-avec-0-jobsh)
- [V. Vérifiez vos acquis](#v-vérifiez-vos-acquis)

## I. Théorie — comprendre avant d'exécuter

### A. Le problème que résout MapReduce

Un programme séquentiel lit ses données, les traite, puis écrit son résultat. Tant que les données tiennent dans la mémoire et que le temps de calcul reste raisonnable, c'est la bonne approche. À l'échelle du Big Data, trois limites apparaissent :

- **la mémoire** : un fichier de plusieurs téraoctets ne tient pas dans la RAM d'une machine ;
- **le temps de calcul** : traiter les données sur un seul cœur prendrait des jours ;
- **le réseau** : rapatrier toutes les données vers une seule machine coûte plus cher que de les traiter sur place.

L'idée fondatrice de MapReduce est donc simple : **déplacer le calcul vers les données**, pas les données vers le calcul. Chaque machine traite le fragment qu'elle héberge déjà, puis seuls les résultats partiels — petits — circulent sur le réseau.

![MapReduce](assets/mr.png)

Un job MapReduce s'écrit avec deux fonctions :

- **map** : projette chaque donnée d'entrée en une ou plusieurs paires `clé → valeur` ;
- **reduce** : agrège toutes les valeurs associées à une même clé.

Le reste est pris en charge par le framework : découpage de l'entrée, distribution des tâches, partition, tri, transfert des données, reprise en cas d'échec. **YARN** fournit la couche de ressources : il alloue les conteneurs dans lesquels tournent les mappers et les reducers.

> **MapReduce est un traitement batch.** Il a un coût de démarrage incompressible — 35 à 69 s sur le cluster du cours, même pour un petit fichier. Ce n'est pas un moteur de requête interactive : on prépare son job, on le teste en local, puis on le lance.

### B. WordCount sans code : le principe

Le **WordCount** est le « Hello World » de MapReduce : il compte le nombre d'occurrences de chaque mot dans un texte. Il suffit à comprendre le modèle.

Prenons un texte d'une ligne :

```text
le chat le chien le chat
```

**1. La phase map** découpe le texte en mots et émet, pour chaque mot, la paire `(mot, 1)` :

| Entrée | Paires émises |
|---|---|
| `le chat le chien le chat` | `(le,1) (chat,1) (le,1) (chien,1) (le,1) (chat,1)` |

Le mapper **ne compte rien** : il étiquette chaque occurrence. Il ne voit que la ligne ou le fragment qu'on lui a donné, donc il ne peut pas savoir combien de fois « le » apparaît dans le reste du fichier.

**2. Le shuffle et le tri** regroupent les paires par clé et trient les clés :

| Clé | Valeurs reçues |
|---|---|
| `chat` | `1, 1` |
| `chien` | `1` |
| `le` | `1, 1, 1` |

**3. La phase reduce** additionne les valeurs de chaque groupe et émet le résultat final :

| Clé | Sortie |
|---|---|
| `chat` | `2` |
| `chien` | `1` |
| `le` | `3` |

Trois idées à retenir :

1. **Le mapper étiquette, le reducer compte.** Le mapper ne peut pas agréger globalement, le reducer ne peut pas voir les lignes.
2. **Tout passe par la clé.** C'est elle qui décide du regroupement, donc du résultat.
3. Le même schéma sert à une somme, un comptage, une moyenne (somme + compte), un top-k (tri), etc.

![Récapitulatif du WordCount : deux mappers, shuffle, deux reducers](assets/mr-word-count-2.png)

### C. Du fichier au mapper : qui découpe quoi ?

C'est ici que se joue la confusion la plus fréquente du chapitre. Trois couches interviennent, et elles ne font pas le même travail.

| Couche | Ce qu'elle découpe | Unité | Ce qu'elle décide | Conséquence |
|---|---|---|---|---|
| **HDFS** | le fichier en blocs | bloc (128 Mio par défaut) | stockage, réplication | un fichier = n blocs |
| **MapReduce** | l'entrée en splits | split | nombre de tâches map | 1 split = 1 map |
| **YARN** | rien | conteneur | mémoire/CPU, placement | 1 tâche = 1 conteneur |

**HDFS découpe le fichier en blocs.** C'est une unité de **stockage** : le NameNode connaît les blocs, la réplication duplique les blocs, les DataNodes les stockent. Un bloc n'est pas une unité de calcul. Le CSV Spotify du cours fait 155 593 octets : il tient dans **un seul bloc** de 128 Mio.

**MapReduce découpe l'entrée en splits.** Un split est une **tranche logique de lecture**, pas une copie physique des données. Pour chaque split, Hadoop crée une tâche map : **un split = une tâche map = un mapper**. Un split peut être plus petit qu'un bloc, et plusieurs mappers peuvent donc lire le même bloc. C'est l'`InputFormat` qui décide du découpage, pas YARN.

**YARN alloue des conteneurs.** Un conteneur est un ensemble de ressources (mémoire, CPU) réservé sur un nœud pour exécuter une commande. Chaque tâche map ou reduce tourne dans un conteneur. YARN ne lit pas le fichier, ne connaît pas les splits et ne trie rien : il fournit les ressources et place les conteneurs au plus près des données quand c'est possible.

La chaîne complète est donc :

```text
fichier HDFS → blocs (HDFS) → splits (MapReduce) → tâches map (MapReduce) → conteneurs (YARN)
```

#### Combien de mappers ?

La littérature dit souvent : « nombre de splits = taille du fichier / taille de bloc ». C'est une approximation. Sur **notre** cluster, la mesure ne suit pas toujours cette formule.

Ce qui a été mesuré :

| Situation | Splits / mappers observés |
|---|---|
| une `-input` d'**un seul bloc** (155 593 o), sans option | **2 splits** |
| même fichier, `-D mapreduce.job.maps=1` | **1 split** |
| même fichier, `-D mapreduce.job.maps=4` | **4 splits** (toujours le même bloc unique) |
| fichier de 1 095 829 o (1 bloc), `-D mapreduce.job.maps=8` | **8 splits**, `Map input records=6678` |

Deux enseignements :

1. Le nombre de splits dépend de la **configuration d'entrée** — et, sur ce cluster, du hint `mapreduce.job.maps` — autant que de la taille des données.
2. **Un mapper = un split, mais pas forcément un bloc** : plusieurs mappers peuvent lire le même bloc.

> **La règle à retenir : le nombre de mappers se lit, il ne se devine pas.** Après lancement, la ligne `number of splits:` du log donne la réponse. La commande complète est en II.E ; ici, retenez le réflexe : `2>&1 | grep "number of splits"`.

#### Combien de reducers ?

Par défaut : **1 reducer**, et donc **un seul fichier de sortie** : `part-00000`. Pour obtenir plusieurs fichiers (`part-00000`, `part-00001`, …), demandez plusieurs reducers :

```bash
-D mapreduce.job.reduces=2
```

Chaque reducer produit son propre fichier. Conséquence importante : avec plusieurs reducers, **le résultat n'est plus globalement trié** — chaque fichier est trié, mais l'ordre entre les fichiers n'est pas garanti. Avec un seul reducer, la sortie est globalement triée et tient dans un fichier unique, ce qui est plus simple à relire.

### D. YARN : le gestionnaire de ressources

YARN n'est pas une annexe de MapReduce : c'est la couche qui décide si votre job démarre, attend, ou échoue faute de mémoire.

#### Pourquoi YARN ?

Dans Hadoop 1, un seul démon — le JobTracker — gérait à la fois les ressources du cluster et l'exécution des jobs MapReduce. Hadoop ne savait exécuter que MapReduce, et le passage à l'échelle posait problème.

**YARN** (*Yet Another Resource Negotiator*), arrivé avec Hadoop 2.0 (2012-2013), découple les deux :

- la **gestion des ressources** est confiée au ResourceManager et aux NodeManagers ;
- l'**exécution d'une application** est confiée à un ApplicationMaster et à des conteneurs.

Conséquence : le même cluster peut héberger MapReduce, mais aussi Spark, Hive, Tez ou d'autres applications. YARN ne sait pas ce qu'est un mapper : il sait allouer des ressources à une application qui lui en fait la demande.

#### Les trois rôles

| Composant | Où | Rôle |
|---|---|---|
| **ResourceManager** (RM) | sur le master | alloue les ressources, planifie les applications, suit leur état |
| **NodeManager** (NM) | sur chaque nœud | lance et surveille les conteneurs, remonte l'état au RM |
| **ApplicationMaster** (AM) | un par application | négocie les conteneurs auprès du RM, orchestre les tâches de *son* application |
| **Conteneur** | sur un nœud | mémoire + CPU + commande à exécuter ; une tâche map ou reduce y tourne |

![RessourceManager](assets/ressourcemanger.jpeg)

#### Le cycle de vie d'une application

1. Le client soumet l'application au **ResourceManager**.
2. Le RM alloue un conteneur pour l'**ApplicationMaster** sur un NodeManager.
3. L'AM démarre, s'enregistre auprès du RM et prend la direction de l'application.
4. L'AM demande des conteneurs pour les tâches map et reduce ; le scheduler du RM les alloue sur les nœuds.
5. L'AM lance les tâches dans les conteneurs ; les NodeManagers surveillent leur mémoire et leur état.
6. Les tâches rendent compte à l'AM ; l'AM rend compte au RM. À la fin, les conteneurs sont libérés et l'historique est conservé par le JobHistoryServer.

> **Qui planifie quoi ?** Le **scheduler du ResourceManager** alloue des conteneurs **aux applications**. L'**ApplicationMaster** planifie les **tâches de son application** : c'est lui qui décide combien de maps et de reduces lancer, et dans quel ordre. Les deux niveaux ne se confondent pas.

#### Les schedulers YARN

YARN propose plusieurs politiques d'allocation. Elles ne changent pas le résultat de votre job, mais elles changent **qui attend** quand plusieurs applications tournent en même temps.

| Politique | Principe | Avantage | Limite |
|---|---|---|---|
| **FIFO** | premier arrivé, premier servi ; une seule file | simple à comprendre | un gros job peut bloquer tous les suivants |
| **Capacity** | files d'attente avec des capacités garanties ; la capacité inutilisée peut être partagée | multi-tenant, cloisonnement | configuration plus lourde |
| **Fair** | chaque application obtient une part équitable des ressources, ajustée dynamiquement | bon pour les charges mélangées | moins prévisible pour les gros batchs |

**Capacity Scheduler est le défaut** dans Hadoop 3. La stratégie se configure dans `etc/hadoop/yarn-site.xml`, propriété `yarn.resourcemanager.scheduler.class`.

![Scheduling policies](assets/scheduling-policies.jpeg)

> **Scénario concret.** Trois jobs arrivent en même temps. En FIFO, le troisième attend que les deux premiers aient fini. En Capacity, chaque file avance selon sa capacité garantie. En Fair, les trois se partagent les ressources disponibles. Sur un cluster de cours à trois nœuds, l'effet est modeste — mais il explique pourquoi un job peut rester `ACCEPTED` alors que le cluster semble libre.

#### La mémoire et les conteneurs

YARN raisonne en **conteneurs**, pas en fichiers. Trois réglages expliquent la plupart des jobs qui attendent ou échouent :

| Réglage | Rôle | Exemple sur le cluster du cours |
|---|---|---|
| `yarn.nodemanager.resource.memory-mb` | mémoire totale offerte par un NodeManager | **256 Mio** |
| `mapreduce.map.memory.mb` | mémoire demandée par une tâche map | **192 Mio** |
| `mapreduce.reduce.memory.mb` | mémoire demandée par une tâche reduce | à relever dans `mapred-site.xml` |
| `yarn.scheduler.maximum-allocation-mb` | taille maximale d'un conteneur | à relever dans `yarn-site.xml` |

Avec 256 Mio par NodeManager et 192 Mio par map, **un NodeManager ne peut faire tourner qu'une map à la fois**. Le parallélisme vient du nombre de nœuds, pas de la mémoire accordée à chaque tâche. Pour un job qui a besoin de plus de mémoire, on augmente `mapreduce.map.memory.mb` **et** on vérifie que `yarn.scheduler.maximum-allocation-mb` l'autorise ; sinon le conteneur est refusé. Si une tâche dépasse la mémoire de son conteneur, le NodeManager la tue : cherchez `Container killed` dans les logs.

![yarn-config](assets/yarn-config.png)

> **Ce que YARN ne fait pas** : il ne lit pas les données, ne découpe pas les fichiers en splits, ne trie pas les clés et n'exécute pas votre code à leur place. Il alloue des ressources et fait tourner des conteneurs.

### E. Entre le map et le reduce : partition, shuffle, tri

![Le pipeline MapReduce : blocs, splits, map, partition, shuffle/sort, reduce](assets/mapreduce-pipeline.svg)

Le mapper écrit des paires `clé\tvaleur`, le reducer lit des lignes `clé\tvaleur` groupées. Entre les deux, **trois opérations que vous ne codez pas** décident pourtant du résultat.

**1. Partition — où va chaque paire ?** Chaque clé est routée vers un reducer par un hash. **Toutes les valeurs d'une même clé arrivent au même reducer**, et une clé ne se coupe jamais entre deux reducers. Conséquences : avec plusieurs reducers, chaque fichier `part-*` est trié mais **aucun ne contient le classement global** (déjà vu en I.C) ; et une clé très fréquente — un mot présent dans chaque ligne — charge **un seul** reducer, que les autres n'aident pas.

**2. Shuffle — quand le reducer peut-il commencer ?** Les paires traversent le réseau, du mapper vers le reducer. Le reducer peut récupérer au fur et à mesure, mais il **ne peut pas passer à la phase reduce** tant que tous les mappers n'ont pas fini : il lui faut toutes les valeurs de chaque clé. Conséquences : un job affiché à `reduce 0%` n'est pas en panne, il attend un mapper ; un seul mapper lent retarde tout le job. C'est aussi la phase la plus coûteuse en réseau — d'où le combiner.

**3. Tri — pourquoi le reducer n'a-t-il pas besoin de trier ?** Chaque reducer trie ses paires **par clé** avant de les passer à votre script. Conséquence directe : le reducer se contente d'accumuler les lignes **consécutives** de même clé — c'est le `current_*` de II.C. Attention, le tri est **lexicographique** : `"10"` passe avant `"2"`, `"2023"` avant `"897"`. D'où le piège des années (I.C) et le même comportement en HBase (séquence 06).

![Avant le tri : les paires arrivent dans le désordre](assets/mapreduce_unsort.png) ![Après le tri : les clés sont regroupées](assets/mapreduce_sort.png)

> **Ce que vous n'avez pas à coder** : le partitionneur par défaut suffit à tous les exercices du cours. Ce que vous devez savoir, c'est ce que ces phases **imposent à votre code** : aucun tri, aucun état entre deux appels, une clé = un seul reducer.

| Ce que le framework garantit | Ce que vous garantissez |
|---|---|
| découpage en splits, distribution, partition, shuffle/tri, reprise d'une tâche | le contrat `clé\tvaleur`, un parsing robuste, pas d'état partagé entre tâches |

#### Le combiner : un mini-reducer local

Le **combiner** est un « mini-reducer » exécuté **sur le serveur du mapper**, avant le shuffle, pour alléger le trafic réseau : il agrège localement ce que le mapper vient d'émettre. C'est une optimisation, pas une obligation — et Hadoop peut le relancer ou l'omettre, donc il ne doit jamais changer le résultat.

Un combiner suppose une propriété mathématique : les valeurs partielles doivent se combiner sans perdre d'information. La somme et le compte en bénéficient — **pas la moyenne** :

```text
moyenne(10, 20, 30) = 20
combiner : groupe1 = moyenne(10, 20) = 15  ;  groupe2 = moyenne(30) = 30
moyenne(15, 30) = 22,5   ≠   20   ✗
```

Règle : **pour une moyenne, on ne combine jamais des moyennes.** On fait remonter **somme + compte**, et on divise **à la fin du reducer**. C'est exactement le pattern demandé par le « moyenne par année » de l'EX 04 — et la moyenne des moyennes est la signature d'un bug classique à repérer dans les rendus.

### F. Ce que le modèle impose au code

La conséquence la plus importante du modèle — et celle qu'on oublie le premier jour — est que **chaque tâche map et chaque tâche reduce est un processus isolé, lancé de zéro**. Un mapper ne connaît que ce qui passe sur son `stdin`, un reducer de même.

Cela impose cinq règles :

1. **Pas d'état partagé entre tâches.** Une variable globale, un fichier local ou un cache en mémoire ne sont pas partagés entre deux mappers. Pour échanger des données entre tâches, on passe par HDFS ou par le flux clé/valeur.
2. **Le seul état toléré est local à la tâche.** Le pattern `current_key` du reducer est un état local au processus : il vit le temps d'une tâche, puis disparaît.
3. **L'en-tête d'un CSV n'existe que dans le premier split.** Il n'est lu que par le premier mapper à lire la première ligne du premier split. Les autres mappers ne le voient jamais. On l'écarte donc **par la valeur** (`if row[0] == 'track_name': continue`), **jamais** par un `next(csv.reader(...))` : `next()` ne filtrerait que le premier mapper et ferait disparaître une ligne de données chez les autres.
4. **Un reducer = un fichier de sortie.** Avec N reducers, le résultat est éclaté en N fichiers `part-*` et le tri global est perdu.
5. **L'échec d'une tâche n'est pas l'échec du job.** L'ApplicationMaster relance la tâche ailleurs (jusqu'à un nombre de tentatives configurable). Mais les compteurs d'un job en échec sont **partiels** : ils ne comptent que les tâches qui ont survécu. Ne les lisez jamais comme ceux d'un job réussi.

> **Et la latence ?** Même un petit job coûte 35 à 69 s sur le cluster du cours : allocation des conteneurs,上传 des scripts, lancement des JVM, écriture sur HDFS. C'est la raison n°1 de **tester en local d'abord** : chaque aller-retour cluster en échec coûte ce temps-là.

## II. Application simple — le premier job à la main

### A. Le contrat Hadoop Streaming

MapReduce est écrit en Java, mais grâce à **Hadoop Streaming**, on écrit ses mappers et reducers dans **n'importe quel langage** (Python, Ruby, Bash…) : le jar Streaming lit l'entrée sur `stdin` de vos programmes et collecte leur `stdout`. Le contrat tient en trois lignes :

- le **mapper** lit des lignes sur `stdin`, écrit sur `stdout` des paires `clé\tvaleur` (une par ligne) ;
- Hadoop fait la **partition** et le **shuffle/sort** entre les deux ;
- le **reducer** lit sur `stdin` les paires `clé\tvaleur` **déjà groupées et triées par clé**, écrit le résultat agrégé sur `stdout`.

Le jar est fourni avec Hadoop : `hadoop-streaming-<version>.jar` (sur le cluster du cours, Hadoop 3.3.6), dans `$HADOOP_HOME/share/hadoop/tools/lib`.

L'option `-file` envoie un script local au cluster. Il est localisé dans le répertoire de travail de la tâche **sous son nom de base** : si vous envoyez `scripts/mapper.py`, la commande devra référencer `mapper.py`, pas `scripts/mapper.py`.

> **Hadoop Streaming n'est pas une bibliothèque Python.** C'est un contrat de flux texte : votre programme est lancé comme une commande, une fois par tâche, et communique par `stdin`/`stdout`. Ce qui fait foi, c'est le format `clé\tvaleur`, pas le langage.

### B. Structure minimale du mapper

Un mapper minimal tient en cinq lignes :

```python
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, value = ...   # votre extraction
    print(f"{key}\t{value}")
```

Pour le WordCount, l'extraction tient en trois lignes :

```python
import sys

for line in sys.stdin:
    for word in line.strip().split():
        if word:
            print(f"{word.lower()}\t1")
```

Trois règles pour le mapper :

- **une ligne = une paire** `clé\tvaleur`, séparée par **une tabulation** ;
- **pas de `print` de debug sur `stdout`** : il serait interprété comme une donnée. Utilisez `stderr` (`print(..., file=sys.stderr)`) ;
- **protégez les conversions** (`int()`, `float()`) : sur le cluster, une exception tue la tâche entière.

### C. Structure minimale du reducer

Le reducer minimal est toujours le même : accumuler les valeurs tant que la clé ne change pas, émettre au changement de clé, puis ne pas oublier la dernière clé.

```python
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue
    word, count_str = parts

    try:
        count = int(count_str)
    except ValueError:
        continue

    if word == current_word:          # même clé : on accumule
        current_count += count
    else:                             # nouvelle clé : on émet la précédente
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

if current_word is not None:          # ne pas oublier la dernière clé
    print(f"{current_word}\t{current_count}")
```

Pourquoi cette structure ?

- **Les clés arrivent groupées et triées.** C'est le shuffle qui s'en charge. Le reducer n'a donc pas à trier : il lui suffit de comparer la clé courante à la précédente.
- **Le flush final est obligatoire.** Sans le `if current_word is not None` après la boucle, la dernière clé n'est jamais émise. C'est l'erreur classique du premier reducer.
- **Le `try/except` évite qu'une ligne sale tue la tâche.** Sur le cluster, une exception Python fait échouer la tâche, puis le job.
- **Le reducer émet une paire par clé**, pas une paire par valeur.

### D. Tester en local avant le cluster

Simuler le pipeline MapReduce avec des pipes Unix est l'habitude la plus rentable du développement Hadoop. Créez un fichier `mots.txt` :

```text
bonjour bonsoir bonjour
bonsoir bonjour
```

Testez le mapper seul :

```bash
cat mots.txt | python3 mapper.py
```

```text
bonjour	1
bonsoir	1
bonjour	1
bonsoir	1
bonjour	1
```

Testez le pipeline complet, avec le `sort` qui simule le shuffle :

```bash
cat mots.txt | python3 mapper.py | sort | python3 reducer.py
```

```text
bonjour	3
bonsoir	2
```

Sans le `sort`, le reducer reçoit les clés dans l'ordre du mapper (`bonjour, bonsoir, bonjour…`) et émet à chaque changement : il ne regroupe pas, le comptage est faux. Le `sort` du shell **simule le shuffle** : sur le cluster, Hadoop regroupe et trie les clés avant le reducer.

> **Si votre code fonctionne avec `| sort |`, il fonctionnera sur le cluster** — et c'est aussi le moyen de voir les paires `clé\tvaleur` telles que le reducer les recevra.

> **Sous Windows** (avant de se connecter au master) : `type mots.txt | python mapper.py | sort | python reducer.py`.

Le test local est **nécessaire mais pas suffisant** : le `sort` GNU n'est pas le tri Hadoop (encodage, séparateur `\t`), et le cluster a des contraintes de mémoire et de parallélisme que votre poste n'a pas. Il reste le meilleur moyen de ne pas payer 40 s de cluster pour une virgule oubliée. Le [TD01](td/TD01.md) propose les trois variants du pipe ; ici, on passe au fichier réel.

### E. La commande `hadoop jar` la plus simple

On suppose HDFS et YARN démarrés (`./start-dfs.sh` puis `./start-yarn.sh`, ou `./start-all.sh`). Vérifiez avec `jps` que les démons sont là, puis déposez le fichier d'entrée dans HDFS :

```bash
hdfs dfs -mkdir -p input
hdfs dfs -put -f mots.txt input
hdfs dfs -ls input
```

Vérifiez que le jar Streaming est présent :

```bash
ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar
```

Lancez le job :

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file mapper.py -mapper "python3 mapper.py" \
  -file reducer.py -reducer "python3 reducer.py" \
  -input input/mots.txt \
  -output output/wordcount
```

Les cinq paramètres à connaître :

- `-file` : envoie le script local au cluster (il sera exécuté sur les nœuds) ;
- `-mapper` / `-reducer` : la commande exacte exécutée pour chaque tâche ;
- `-input` / `-output` : chemins **HDFS** (le répertoire de sortie **ne doit pas exister**).

Pendant le lancement, Hadoop écrit beaucoup de lignes. Les plus utiles :

```text
INFO mapreduce.JobSubmitter: number of splits:2
INFO mapreduce.Job: Running job: job_1705417740450_0001
INFO mapreduce.Job: map 100% reduce 100%
INFO mapreduce.Job: Job job_1705417740450_0001 completed successfully
```

Pour isoler le nombre de splits :

```bash
hadoop jar … 2>&1 | grep "number of splits"
# 22/09/11 10:31:42 INFO mapreduce.JobSubmitter: number of splits:2
```

Le résultat est un répertoire HDFS, avec un fichier par reducer :

```bash
hdfs dfs -ls output/wordcount
# Found 2 items
# -rw-r--r--   1 root supergroup  ... output/wordcount/_SUCCESS
# -rw-r--r--   1 root supergroup  ... output/wordcount/part-00000

hdfs dfs -cat output/wordcount/part-00000
# bonjour	3
# bonsoir	2
```

- `_SUCCESS` est un fichier vide : c'est le **marqueur de fin** du job. S'il est absent, le job n'a pas abouti.
- `part-00000` contient les données. Un seul reducer = un seul fichier. Avec `-D mapreduce.job.reduces=2`, vous auriez `part-00000` et `part-00001`.
- `hdfs dfs -tail` ne montre que le dernier kilo-octet : pour un petit résultat, préférez `-cat`. Pour récupérer et fusionner plusieurs `part-*` en local : `hdfs dfs -getmerge output/wordcount resultat.txt`.

Le ResourceManager de YARN expose une interface web : **<http://localhost:8088>**. On y suit l'état (`ACCEPTED`, `RUNNING`, `SUCCEEDED`, `FAILED`), la progression, et — réflexe n°1 quand un job échoue — les liens vers les logs détaillés. Le comportement des nœuds esclaves est visible sur **<http://localhost:8041>** (slave1) et **<http://localhost:8042>** (slave2) : mémoire, conteneurs actifs, tâches en cours.

![MapReduce Process](assets/mr-process.jpeg) ![MapReduce Stats](assets/mr-stats.jpeg)

### F. Les 3 avertissements du premier lancement

| Piège | Message typique | Correctif |
|---|---|---|
| Le dossier de sortie **existe**, même vide | `FileAlreadyExistsException: Output directory … already exists` | `hdfs dfs -rm -r -f output/wordcount` avant chaque run |
| `-file` et `-mapper` ne référencent pas le **même nom** | `python3: can't open file …` | même nom de base : `-file mapper.py` + `-mapper "python3 mapper.py"` |
| L'entrée n'est **pas dans HDFS** | `Input path does not exist: hdfs://…` | `hdfs dfs -ls input`, puis `-put` |

**1. Le dossier de sortie ne doit pas exister — même vide.** Hadoop ne vérifie pas si le dossier est vide : il vérifie s'il **existe**. Ne faites donc pas `hdfs dfs -mkdir output/wordcount` : `hadoop jar` crée le dossier lui-même. Si vous relancez le job, supprimez d'abord l'ancien résultat :

```bash
hdfs dfs -rm -r -f output/wordcount
```

C'est exactement ce que fait la ligne `hdfs dfs -rm -r -f output/wordcount` du script `0-job.sh` (partie III). C'est l'erreur n°1 du premier lancement.

**2. Le nom du script doit être cohérent entre `-file` et `-mapper`/`-reducer`.** `-file` localise le script sous son **nom de base**. Si vous écrivez `-file scripts/mapper.py` puis `-mapper "python3 scripts/mapper.py"`, la tâche cherchera `scripts/mapper.py` dans son répertoire de travail, où le fichier s'appelle `mapper.py` : elle échouera avec `can't open file`. Utilisez le même nom des deux côtés.

**3. L'entrée est un chemin HDFS, pas un chemin local.** `-input input/mots.txt` désigne `/user/<vous>/input/mots.txt` dans HDFS, pas le `mots.txt` de votre poste. Vérifiez avec `hdfs dfs -ls input` et déposez le fichier avec `hdfs dfs -put` si besoin.

**Checklist avant de lancer** :

- [ ] mapper et reducer testés en local avec `| sort |` ;
- [ ] fichier d'entrée présent dans HDFS (`hdfs dfs -ls input`) ;
- [ ] dossier de sortie absent (`hdfs dfs -ls output/wordcount` doit échouer) ;
- [ ] noms cohérents entre `-file`, `-mapper` et `-reducer` ;
- [ ] jar Streaming trouvé (`ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar`) ;
- [ ] HDFS et YARN démarrés (`jps`).

**Quelques pièges bonus** : le séparateur est une **tabulation**, pas un espace ; un `print` de debug sur `stdout` corrompt le flux ; `python3` doit exister sur les nœuds ; un script écrit sous Windows avec des fins de ligne CRLF peut produire `can't open file 'mapper.py\r'` — convertissez en LF.

### G. Quand ça ne marche pas : lire YARN

Un job en échec, c'est un job dont on a le *résultat* — pas forcément le *diagnostic*. L'ordre de lecture est toujours le même :

1. **le message du client** : il donne souvent la première piste (`subprocess failed with code 1`, `Output directory already exists`…) ;
2. **l'interface 8088** : l'application, son état, ses tentatives, le lien vers les logs ;
3. **`yarn logs`** : le `stderr` réel de la tâche qui a planté ;
4. **les compteurs** : avec prudence, surtout si le job a échoué.

Quand l'UI 8088 ne suffit plus, on va chercher la preuve dans les journaux :

```bash
yarn logs -applicationId application_1690000000000_0001 2>&1 | grep -n "PipeMapRed\|stderr\|Exception" | head
```

- `application_…` : l'identifiant affiché dans l'UI 8088 (et dans la sortie du launcher, après `INFO mapreduce.JobSubmitter`) — prenez-le **avant** l'échec ou dans l'historique.
- L'option `-containerId` cible un conteneur précis (ex. `container_e1_1690000000000_0001_01_000002`) ; sans elle, `yarn logs` affiche les journaux de **toutes** les tâches. Pour lister les conteneurs avec leur nœud : `-show_application_log_info`.

#### Les compteurs d'un job en échec sont partiels

Deux cas mesurés sur le cluster :

**1. `int(row[8])` non protégé** (ligne 576 du CSV, champ `streams` corrompu) :

```text
PipeMapRed.waitOutputThreads(): subprocess failed with code 1
```

Le job échoue après plusieurs tentatives. Les compteurs affichent `Map input records=473` — **le seul task survivant**, pas les 954. **Ne lisez jamais les compteurs d'un job en échec comme ceux d'un job réussi** : un compteur n'est fiable que si l'on sait *quel* task l'a monté.

**2. `next(csv.reader(...))` pour sauter l'en-tête** : le job **réussit**, `Map input records` est **identique** (954), mais il manque une paire (1480 contre 1481). Les compteurs ne le montrent pas — c'est en comparant à **une valeur attendue** (par exemple le `COUNT(*)` de Hive, ou le total calculé en Python) qu'on le détecte.

Il y a donc **deux sortes d'échec** : l'échec *visible* (yarn logs, comme le `subprocess failed with code 1`) et l'échec *silencieux* (job vert, résultat faux d'une paire). Le second exige de disposer d'une **valeur de contrôle** écrite avant le job.

> **Le test local est votre premier diagnostic.** Si le mapper plante sur `mots.txt`, il plantera sur le cluster. Si le reducer ne regroupe pas sans `sort`, il ne regroupera pas non plus sur le cluster. Corrigez localement, puis relancez : chaque relance coûte 35 à 69 s.

## III. Simplifier les exécutions — le job reproductible

### A. Pourquoi automatiser (et ce qui reste manuel)

La chaîne manuelle de la partie II tient en six étapes :

1. démarrer HDFS et YARN ;
2. créer le répertoire d'entrée dans HDFS ;
3. copier le fichier d'entrée ;
4. supprimer l'ancien répertoire de sortie ;
5. lancer `hadoop jar` ;
6. lire le résultat.

Répétée à chaque essai, cette chaîne est une source d'erreurs : on oublie de supprimer l'output, on se trompe de nom de fichier, on relance sans avoir testé en local. Un script shell met ces commandes dans un fichier, dans le bon ordre, avec des variables. Il ne comprend pas votre job : il **répète** ce que vous avez compris.

Ce qui reste manuel :

- choisir le mapper et le reducer ;
- vérifier que le résultat est correct ;
- lire les logs et diagnostiquer ;
- décider de la prochaine étape.

### B. La structure fournie

Les scripts du cours sont dans [`annexes/utils/`](annexes/utils/). Pour ce chapitre, les plus utiles sont :

| Fichier | Rôle | Quand l'utiliser |
|---|---|---|
| `0-job.sh` | version outillée : arguments, contrôles, démarrage, `put`, suppression de l'output, `hadoop jar`, `_SUCCESS`, lecture | à chaque run |
| `1-structure-mapper.py` | squelette minimal de mapper | partir de zéro |
| `2-structure-reducer.py` | squelette minimal de reducer | agrégation type somme |
| `3-mapper-txt.py` | mapper WordCount sur fichier texte | partie II |
| `4-mapper-csv-reader.py` | mapper CSV Spotify avec `csv.reader` | exercices |
| `5-reducer-hbase.py` | reducer qui écrit dans HBase | séquence 06 |
| `6-reducer-matplotlib-pdf.py` | reducer qui génère un PDF | exercice 5 |
| `7` à `13` | préparation Hive, TP final, HBase | séquences suivantes |

Une structure de travail simple :

```text
mon-projet/
├── 0-job.sh
├── mapper.py
├── reducer.py
└── mots.txt          # ou Spotify_Most_Streamed_Songs.csv
```

`0-job.sh` utilise des chemins **relatifs** : il doit être lancé depuis le répertoire qui contient `mapper.py`, `reducer.py` et le fichier d'entrée. Si vous le copiez depuis `annexes/utils/`, copiez aussi les scripts que vous utilisez, ou adaptez les chemins dans le script. Les scripts `7` à `13` ne servent pas dans ce chapitre : ne les lancez pas « pour voir ».

### C. Le script minimal, commenté ligne par ligne

Voici le script minimal, celui que vous devez pouvoir réécrire de mémoire après la partie II. Il est taillé pour le WordCount et `mots.txt` :

```bash
#!/bin/bash
# 0-job.sh — version minimale, pour le WordCount de la partie II.

./start-dfs.sh
./start-yarn.sh

hdfs dfs -mkdir -p input
hdfs dfs -put -f mots.txt input
hdfs dfs -rm -r -f output/wordcount

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file mapper.py  -mapper "python3 mapper.py" \
  -file reducer.py -reducer "python3 reducer.py" \
  -input input/mots.txt \
  -output output/wordcount

hdfs dfs -cat output/wordcount/part-00000
```

Ce script ne fait que **recopier dans l'ordre** les commandes de la partie II. Rien n'y est magique : chaque ligne correspond à une étape que vous avez déjà exécutée à la main.

| Étape manuelle (II) | Ligne du script |
|---|---|
| Démarrer HDFS/YARN | `./start-dfs.sh`, `./start-yarn.sh` |
| Créer `input` | `hdfs dfs -mkdir -p input` |
| Copier le fichier | `hdfs dfs -put -f mots.txt input` |
| Supprimer l'output | `hdfs dfs -rm -r -f output/wordcount` |
| Lancer le job | `hadoop jar …` |
| Lire le résultat | `hdfs dfs -cat …/part-00000` |

Trois remarques :

- **`./start-*` et pas `start-*`** : le script `start-dfs.sh` d'Hadoop est dans `$HADOOP_HOME/sbin`, pas dans le `PATH`. Le `./` force l'exécution du script du cours.
- **`hdfs dfs -rm -r -f` avant le job** : c'est la réponse au warning n°1 de II.F. Le script ne crée pas `output/wordcount` ; il laisse Hadoop le créer.
- **`-file` et `-mapper` portent le même nom de base** : `-file mapper.py` dépose le fichier sous le nom `mapper.py` dans le répertoire de la tâche, et `-mapper "python3 mapper.py"` le retrouve. Si vos scripts sont dans `annexes/utils/`, gardez la cohérence : `-file annexes/utils/4-mapper-csv-reader.py -mapper "python3 4-mapper-csv-reader.py"`.

> Ce script est volontairement **nu** : pas d'arguments, pas de vérifications, pas de `set -e`. C'est un support pour comprendre. La version que vous utiliserez réellement est dans [`annexes/utils/0-job.sh`](annexes/utils/0-job.sh) — elle est décrite en III.D.

### D. La version outillée fournie

Le script [`annexes/utils/0-job.sh`](annexes/utils/0-job.sh) reprend ces six étapes et y ajoute ce qui manque pour un usage répété :

- **des arguments** : `$1` = fichier d'entrée local, `$2` = nom du répertoire de sortie HDFS, avec des valeurs par défaut (`mots.txt`, `wordcount`) ;
- **`set -euo pipefail`** : arrêt immédiat si une commande échoue, si une variable n'est pas définie ou si un pipe échoue ;
- **des vérifications avant de lancer quoi que ce soit** : `HADOOP_HOME`, présence du jar `hadoop-streaming`, présence de `mapper.py`, `reducer.py` et du fichier d'entrée ;
- **une sortie idempotente** : suppression de `output/<nom>` avant chaque run ;
- **un contrôle de succès** : présence de `_SUCCESS` avant d'afficher le résultat ;
- **une sortie lisible** : `hdfs dfs -ls` puis `hdfs dfs -cat output/<nom>/part-*`.

Utilisation, depuis le répertoire qui contient `mapper.py`, `reducer.py` et le fichier d'entrée :

```bash
./0-job.sh                                   # mots.txt -> output/wordcount
./0-job.sh mots.txt wordcount                # équivalent explicite
./0-job.sh Spotify_Most_Streamed_Songs.csv streams_par_annee
```

Deux variables d'environnement permettent d'adapter le script sans l'éditer : `MAPPER` / `REDUCER` (chemins des scripts) et `SKIP_START=1` (ne pas redémarrer HDFS/YARN, déjà lancés ou scripts de démarrage absents du répertoire courant).

L'option `-f` de `-put` et le `-rm -r -f` initial rendent le script **rejouable** : on peut le relancer sans se demander si le dossier d'entrée ou de sortie existe déjà. En cas d'échec du job, `set -e` arrête le script avant le `-cat` : vous ne lisez pas un résultat partiel en croyant lire le résultat final, et vous allez directement voir `yarn logs`.

Pour un nouveau job, quatre choses changent :

1. **l'entrée et la sortie** : les arguments `$1`/`$2` ;
2. **les scripts** : `-file` et `-mapper`/`-reducer` doivent référencer le même nom de base ;
3. **les services** : pas besoin de HBase pour un job qui écrit sur `stdout` ;
4. **la lecture du résultat** : `part-00000` si un seul reducer, `part-*` si plusieurs.

Si le reducer écrit dans HBase, le script `0-job.sh` ne suffit plus : démarrez HBase et Thrift (`./start-hbase.sh`, `./start-thrift.sh`), utilisez `5-reducer-hbase.py`, et supprimez le `hdfs dfs -cat` final — un reducer HBase n'écrit rien sur `stdout`. La table HBase doit être créée **une seule fois, avant le job**, jamais dans le reducer.

### E. Les variantes fournies

| Script | Ce qu'il fait | Point d'attention |
|---|---|---|
| `5-reducer-hbase.py` | agrège puis insère dans HBase | table créée avant le job ; pas de `print` sur `stdout` ; Thrift démarré |
| `6-reducer-matplotlib-pdf.py` | agrège puis génère un PDF | `matplotlib.use('Agg')` avant `pyplot` ; `savefig` après la boucle |
| `7-prepare-spotify-tsv.py` | prépare un TSV pour Hive | séquence 05 |
| `8` à `13` | TP final, HBase, graphiques | séquences suivantes |

Le choix du script dépend de la **sortie** du job : `stdout` (fichier `part-*`), HBase (table), PDF (fichier local poussé dans HDFS). Ne démarrez pas HBase ou Thrift si votre job n'en a pas besoin : c'est du bruit et des ressources consommées pour rien.

### F. Vérifier, rejouer, et ne pas confondre script et orchestrateur

Après un run, vérifiez toujours :

```bash
hdfs dfs -ls output/wordcount        # _SUCCESS + part-00000
hdfs dfs -cat output/wordcount/part-00000
```

- `_SUCCESS` présent = le job a abouti ;
- `part-00000` contient le résultat ;
- comparez avec le test local : mêmes clés, mêmes valeurs.

Pour rejouer, le script supprime l'output avant chaque run. C'est ce qui rend l'exécution **idempotente** : on peut relancer sans se demander si le dossier existe. Sur un cluster partagé, vérifiez aussi ce qui tourne :

```bash
yarn application -list
yarn node -list
jps
```

**Ce que le script ne fait pas** :

- il ne valide pas les données (ligne corrompue, en-tête, type) ;
- il ne relance pas périodiquement (ce n'est pas un cron) ;
- il n'orchestre pas plusieurs jobs dépendants (ce n'est pas Oozie ou Airflow) ;
- il ne prévient personne en cas d'échec : `set -e` arrête le script, mais n'envoie ni mail ni alerte.

> **Règle d'or** : lisez le script avant de le lancer, et sachez à quelle étape de la partie II correspond chaque ligne. Un script est un filet de sécurité, pas une excuse pour ne plus comprendre ce qui se passe.

## IV. Exercices

> Batterie de données de référence (mesurée en session cluster du 11/09) : tout ce qui est en « Résultat attendu » ici a été reproduit au chiffre près — par un job réel, puis recoupé indépendamment en Python et en SQL (Hive).

### A. Dataset et aides

Les exercices suivants approfondissent MapReduce avec des **données réelles** issues de Spotify (nombre de streams, playlists, artistes…) — le même fichier `data/Spotify_Most_Streamed_Songs.csv` que partout dans le cours.

Pour réaliser ces questions, vous utiliserez deux fichiers :

- le **mapper**, pour filtrer et extraire les données ;
- le **reducer**, pour agréger et exporter les données.

**Aides fournies** : des squelettes et scripts de référence sont disponibles dans [`annexes/utils/`](annexes/utils/) :

- `1-structure-mapper.py` / `2-structure-reducer.py` — structure minimale d'un mapper/reducer ;
- `3-mapper-txt.py` — mapper sur fichier texte ;
- `4-mapper-csv-reader.py` — mapper lisant un CSV (gère les virgules quotées) ;
- `0-job.sh` — script d'automatisation d'un job complet (démarrage, put, lancement, résultat).

Le WordCount de la partie II est votre banc d'essai ; ici, on passe au CSV. Trois réflexes à conserver : `csv.reader` plutôt que `split(',')`, en-tête écarté par la valeur, `int()` protégé.

### Exercice 1 — Total des streams par artiste

- Écrire un Job **Map Reduce** permettant, à partir du **CSV Spotify**, de déterminer le **total des streams par artiste**.

Repérez d'abord les colonnes dans le fichier (`head -1` en local) — le CSV a 25 champs, en voici les repères utiles pour cet exercice :

| index | 0 | 1 | 2 | ... | 8 |
| --- | --- | --- | --- | --- | --- |
| champ | `track_name` | `artist(s)_name` | `artist_count` | ... | `streams` |

**Mapper** : extraire le champ artiste (index 1) et `streams` (index 8).

> ⚠️ **`csv.reader`, pas `split(',')`.** Le champ artiste contient des virgules **dans les champs quotés** (`"Latto, Jung Kook"`). Un `split(',')` naïf décale toutes les colonnes qui suivent. Passez par `csv.reader(sys.stdin, delimiter=',')`, qui respecte les guillemets CSV.

**Reducer** : agréger la somme des streams par artiste (même structure que le reducer du WordCount).

**Résultat attendu** (valeurs mesurées — convention : la colonne artiste **telle quelle**, un duo = une clé unique) : **644 artistes**, dont en tête :

```text
The Weeknd	14185552870
Taylor Swift	14053658300
Ed Sheeran	13908947204
Harry Styles	11608645649
Bad Bunny	9997799607
Olivia Rodrigo	7442148916
```

> 🤔 **Question de contrôle — les conventions.** Éclatez les duos (chaque membre se voit créditer du total du titre) et le classement change de tête : **The Weeknd 23 929 760 757**, **Bad Bunny 23 813 527 270**, Ed Sheeran 15 316 587 718, Taylor Swift 14 630 378 183. Deux réponses justes à la même question — **la vôtre dépend du mapper, et c'est vous qui l'écrivez**. Indiquez votre convention dans votre rapport. La séquence 05 (Hive) refait ce calcul en SQL : la validation croisée n'a de sens que si les deux côtés appliquent la même convention.

<details>
<summary>Voir la correction</summary>

Le CSV Spotify a 25 champs ; avec `csv.reader`, l'artiste est `row[1]` et `streams` `row[8]`. Le champ artiste contient des virgules **dans les champs quotés** (`"Latto, Jung Kook"`) : le `csv.reader` est donc non négociable.

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

> ⚠️ **Ligne 576 du CSV** : la ligne de *Love Grows (Where My Rosemary Goes)* a un champ `streams` corrompu (le contenu de 9 colonnes s'y est fondu). Sans le `try/except`, `int(row[8])` lève une exception **sur le cluster** et le job échoue (`PipeMapRed.waitOutputThreads(): subprocess failed with code 1`, compteur `Map input records=473` : seul le task survivant compte — ne lisez jamais les compteurs d'un job en échec comme ceux d'un job réussi).

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

**Sortie attendue** (mesurée) — convention « la colonne artiste telle quelle » (un duo = une clé unique) :

```text
The Weeknd	14185552870
Taylor Swift	14053658300
Ed Sheeran	13908947204
Harry Styles	11608645649
Bad Bunny	9997799607
Olivia Rodrigo	7442148916
```

- **644 artistes** (version non éclatée) ; somme de la colonne `streams` (ligne 576 exclue) : **489 458 828 542**.
- **Version « duos éclatés »** (chaque membre crédité du total du titre) — c'est la question de contrôle de l'énoncé : **698 artistes**, dont The Weeknd **23 929 760 757**, Bad Bunny **23 813 527 270**, Ed Sheeran 15 316 587 718, Taylor Swift 14 630 378 183, Harry Styles 11 608 645 649, Eminem 10 193 727 260. La tête change par *nombre de titres* aussi : Bad Bunny 40, Taylor Swift 38, The Weeknd 37.

> La convention choisie par l'apprenant doit être écrite dans son rapport : c'est elle que la validation croisée avec Hive (EX 05) suppose — `GROUP BY artist_name` en Hive donne exactement la version non éclatée ci-dessus, le `part-00000` du MR éclaté ne doit **pas** lui être comparé tel quel.

</details>

### Exercice 2 — Moyenne des streams par année de sortie

- Écrire un Job **Map Reduce** permettant de déterminer la **moyenne des streams par année de sortie**.

**Mapper** : extraire `released_year` (index 3) et `streams` (index 8).

**Reducer** : accumuler **la somme et le nombre d'occurrences** par année, et émettre la moyenne en fin de groupe — même mécanique que la somme de l'exercice 1, avec deux accumulateurs au lieu d'un. Format de sortie : `année\tmoyenne`.

**Résultat attendu** : une ligne par année de sortie. Vérification de plausibilité : les années s'étendent de **1930 à 2023** (50 années), la plupart des moyennes autour de quelques centaines de millions de streams.

> ⚠️ **Le combiner ne sait pas moyenner.** Une moyenne ne se combine pas : la moyenne des moyennes de deux groupes n'est pas la moyenne globale. Sur ce job, n'utilisez **pas** de combiner — ou alors un combiner « somme + compte » dont le reducer redescend la vraie moyenne.

> ✨ **Lien avec la séquence 05** : l'EX 05 (Hive) demande exactement ce calcul en SQL (`AVG(streams) … GROUP BY released_year`). Vous pourrez confronter vos deux chiffres ligne à ligne.

<details>
<summary>Voir la correction</summary>

Même mapper, avec `released_year = row[3]` à la place de l'artiste. Le reducer accumule **somme et compte** (et non la moyenne au fil de l'eau), puis émet la moyenne à la fin de chaque groupe :

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

- Plage attendue : **1930 → 2023** (50 années). Ordres de grandeur : quelques centaines de millions, **pas** des milliards — si vous tombez sur des valeurs comparables aux sommes de l'exercice 1, la moyenne est mal calculée.
- **Piège** : le combiner ne sait pas moyenner (moyenne des moyennes ≠ moyenne globale). Somme + compte, jamais la moyenne, en combiner.
- **Corollaire d'exécution** : un reducer est un *processus* lancé entre 1 et N fois. La moyenne se calcule en fin de groupe sur les accumulateurs, jamais après coup sur la sortie.

**Validation croisée EX 05** : `SELECT released_year, AVG(streams) … GROUP BY released_year` doit donner les mêmes valeurs ligne à ligne (ligne 576 passée à `NULL` de part et d'autre).

</details>

### Exercice 3 — Nombre total de streams par année de sortie

**Objectif** : Calculer le nombre total de streams par année.

- **Mapper** : Lire le fichier CSV, extraire l'année de sortie (`released_year`) et le nombre de streams (`streams`).
- **Reducer** : Agréger le nombre total de streams par année.

**Résultat attendu** : une ligne par année `année\ttotal_streams`, triée par année croissante. Extrait (valeurs **mesurées** — deux recalculs indépendants sur le CSV réel) :

```text
1975	4206105352
1984	3471625366
...
2023	25808484104
```

> ⚠️ Le nombre de streams est un très grand entier (le maximum mesuré dépasse 3,7 milliards) — attention aux conversions de type et aux valeurs aberrantes de certaines lignes du CSV. La ligne 576 a un champ `streams` corrompu : votre `int()` doit être protégé, sinon le job échoue.

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

> ⚠️ Le CSV Spotify contient des virgules **dans les champs texte** (noms de titres entre guillemets) : passer par `csv.reader` plutôt qu'un simple `split(',')` évite de décaler les colonnes. Les index exacts (`row[3]`, `row[8]`) dépendent de la version du dataset — vérifiez avec `head -1` du CSV.

**reducer.py** : identique au reducer « somme » (somme des streams par clé), en remplaçant la protection d'en-tête par le filtre numérique décrit plus haut.

**Sortie attendue** (extrait — valeurs **mesurées**, à reproduire au chiffre près) :

```text
1975	4206105352
1984	3471625366
...
2023	25808484104
```

Total de la colonne `streams` sur les 953 titres (ligne 576 au `streams` corrompu exclue) : **489 458 828 542** streams.

> Ces trois lignes sont la **clé de voûte de la validation croisée MR ↔ Hive** de l'EX 05 : le `GROUP BY released_year` de Hive doit rendre exactement ces valeurs. Elles ont été recalculées deux fois (Python avec quoting CSV, puis `SUM()` dans Hive) et concordent.
>
> ⚠️ Avec un mapper qui fait `int(row[8])` sans protection, le job **échoue** sur la ligne 576 du CSV (`streams` corrompu, année 1970) : `PipeMapRed.waitOutputThreads(): subprocess failed with code 1`, et les compteurs affichent `Map input records=473` — le task survivant seul, pas la totalité. Ne lisez jamais les compteurs d'un job en échec comme ceux d'un job réussi.

</details>

### Exercice 4 — Nombre moyen de playlists Spotify par année

**Objectif** : Calculer la moyenne du nombre de playlists Spotify contenant des chansons, regroupées par année de sortie.

- **Mapper** : Extraire l'année de sortie (`released_year`) et le nombre de playlists Spotify (`in_spotify_playlists`).
- **Reducer** : Calculer la moyenne du nombre de playlists par année.

**Résultat attendu** : une ligne par année `année\tmoyenne_playlists`. Contrairement à l'exercice 3, votre reducer doit accumuler **à la fois** la somme et le nombre d'occurrences pour calculer la moyenne — même structure que l'exercice 2, reformatée sur une autre colonne.

<details>
<summary>Voir la correction</summary>

Même mapper avec `in_spotify_playlists` à la place de `streams`. Reducer « moyenne » : cf. exercice 2 (somme + compte, moyenne en fin de groupe). Vérification rapide : les moyennes doivent rester dans un ordre de grandeur plausible (quelques centaines de playlists), pas des milliards.

</details>

### Exercice 5 — Statistiques visuelles des streams par année

**Objectif** : Créer un graphique des streams par année, exporté en PDF.

- **Mapper** : Comme dans l'exercice 3, extraire l'année de sortie (`released_year`) et le nombre de streams (`streams`).
- **Reducer** : Agréger le nombre total de streams par année, créer un graphique matplotlib (pie) pour visualiser les tendances et enfin, l'exporter en PDF.

**Résultat attendu** : un fichier PDF contenant le camembert des streams par année, à côté de la sortie standard agrégée. Référence : [`annexes/utils/6-reducer-matplotlib-pdf.py`](annexes/utils/6-reducer-matplotlib-pdf.py).

<details>
<summary>Voir la correction</summary>

Reducer : agrégation identique à l'exercice 3, puis génération matplotlib **à la fin du stdin** (toutes les clés vues) :

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

> **Piège** : `plt.savefig` doit être appelé **après** la boucle de lecture stdin, sinon le PDF est vide. Référence complète : [`annexes/utils/6-reducer-matplotlib-pdf.py`](annexes/utils/6-reducer-matplotlib-pdf.py).

</details>

### Défi — automatiser avec 0-job.sh

Reprenez l'exercice 1 et automatisez-le.

1. Lancez-le à la main, notez le temps et le résultat.
2. Copiez le script minimal de III.C à côté de vos scripts et adaptez-le au CSV Spotify : entrée, sortie, `-file` et `-mapper`/`-reducer`.
3. Lancez-le avec le script. Comparez : même résultat ? Combien de commandes en moins ? Quelles lignes du script correspondent aux avertissements de II.F ?
4. Ouvrez maintenant `annexes/utils/0-job.sh` et repérez ce que la version minimale n'a pas : arguments `$1`/`$2`, `set -euo pipefail`, vérifications, contrôle `_SUCCESS`. Lancez ce script avec des arguments.
5. Provoquez volontairement un échec (fichier d'entrée absent, mauvais nom de mapper) et vérifiez que le script s'arrête au bon endroit, avec un message clair.

**À conserver dans le rapport** : la version du script, le temps des deux exécutions, et une phrase sur ce que le script automatise — et ce qu'il ne fait pas.

> **Pour aller plus loin — le diagnostic.** Retirez le `try/except` autour de `int(row[8])` dans le mapper de l'exercice 1, lancez le job sur le CSV Spotify, puis lisez `yarn logs`. Retrouvez la ligne fautive, expliquez le `Map input records=473`, corrigez et relancez. C'est l'exercice le plus proche d'un incident réel.

## V. Vérifiez vos acquis

*Formatif, jamais noté. Répondez sans relire le chapitre. Les réponses se déplient juste après le séparateur : ce qui compte est de repérer où vous vacillez, pas de faire 8 sur 8.*

**1.** Qui découpe le fichier en *splits* ?

- A. HDFS, au moment du `put`
- B. MapReduce, via l'`InputFormat`
- C. YARN, au moment de lancer l'ApplicationMaster
- D. Le mapper lui-même, à sa première lecture

**2.** Dans YARN, qui négocie les conteneurs auprès du ResourceManager pour une application donnée ?

- A. L'ApplicationMaster, il y en a un pour chaque application
- B. Le NodeManager, présent sur chacun des nœuds du cluster
- C. Le Capacity Scheduler, la politique de planification par défaut
- D. Le mapper lui-même, qui demande la mémoire dont il a besoin

**3.** Quel scheduler est la politique par défaut dans Hadoop 3 ?

- A. FIFO Scheduler
- B. Fair Scheduler
- C. Capacity Scheduler
- D. Aucun : YARN n'a pas de scheduler

**4.** Pourquoi le dossier de sortie d'un job ne doit-il pas exister, même vide ?

- A. Parce que Hadoop refuse d'écrire dans un dossier existant
- B. Parce que HDFS ne sait pas créer de dossier
- C. Parce que le reducer écrirait dans le dossier local du client
- D. Parce que YARN réserve le dossier avant le lancement

**5.** Pourquoi ne pas mettre de combiner sur le job « moyenne des streams par année » ?

- A. Parce que le combiner s'exécute sur le nœud du reducer, après le shuffle : il ne peut rien alléger
- B. Parce que le combiner peut être rejoué après le tri, ce qui ferait compter des valeurs deux fois
- C. Parce que le combiner écarte les lignes mal formées et ferait disparaître la ligne 576 du CSV
- D. Parce que la moyenne de deux moyennes partielles ne rend pas la moyenne globale du groupe

**6.** Pourquoi l'en-tête du CSV doit-il être écarté par la valeur (`if row[0] == 'track_name'`) et non par un `next()` ?

- A. Parce que `next()` lève une exception sur la ligne 576 du CSV, celle au `streams` corrompu, et fait échouer le job
- B. Parce que l'en-tête n'est lu que par le premier mapper : le job réussit et le résultat ne diffère que d'une paire
- C. Parce que `next()` lit deux lignes au lieu d'une sur chaque split, ce qui fausse `Map input records`
- D. Parce que l'en-tête arrive toujours en dernier dans le split, après la totalité des lignes de données

**7.** Comment connaître avec certitude le nombre de mappers d'un job lancé sur le cluster du cours ?

- A. En divisant la taille du fichier par la taille de bloc de 128 Mio, la formule de la littérature
- B. En comptant les blocs HDFS du fichier d'entrée avec `hdfs dfs -ls -R input`
- C. En lisant la ligne `number of splits:` dans le log du job, à son lancement
- D. En regardant combien de fichiers `part-00000` le job a produits dans le répertoire de sortie

**8.** *Question ouverte (2-3 lignes).* Un job échoue avec `subprocess failed with code 1` et les compteurs affichent `Map input records=473`, alors que le même job mené à bout affiche 954. Que s'est-il passé, et pourquoi ce compteur ne montre-t-il pas la totalité ?

---

<details>
<summary>Vérifier ses réponses</summary>

### Réponses

**1. B.** Le découpage en splits est fait par MapReduce via l'`InputFormat` ; HDFS découpe le fichier en blocs, et YARN alloue les conteneurs.
**Pourquoi :** un split est une tranche logique de lecture, pas un bloc physique. HDFS ne connaît que les blocs ; YARN ne connaît que les conteneurs.

**2. A.** L'ApplicationMaster est dédié à une application : il négocie les conteneurs auprès du ResourceManager et orchestre les tâches de son application.
**Pourquoi :** le ResourceManager alloue et planifie à l'échelle du cluster, le NodeManager surveille les conteneurs sur son nœud, et le scheduler n'est qu'une politique — aucun des trois ne pilote une application particulière.

**3. C.** Le Capacity Scheduler est la politique par défaut dans Hadoop 3 ; il organise les ressources en files d'attente avec des capacités garanties.
**Pourquoi :** FIFO est le plus simple mais peut affamer les jobs suivants ; Fair partage équitablement mais est moins prévisible pour les gros batchs. La stratégie se règle via `yarn.resourcemanager.scheduler.class`.

**4. A.** Hadoop vérifie l'existence du dossier de sortie, pas s'il est vide : même un dossier vide fait échouer le job avec `FileAlreadyExistsException`.
**Pourquoi :** `hadoop jar` crée lui-même le dossier de sortie. Il faut le supprimer avant chaque relance (`hdfs dfs -rm -r -f output/wordcount`), ce que fait `0-job.sh`.

**5. D.** Une moyenne ne se combine pas : moyenne(10, 20, 30) = 20, mais moyenne(15, 30) = 22,5.
**Pourquoi :** le combiner agrège localement avant le shuffle, et des valeurs partielles ne se combinent sans perte que pour une somme ou un compte — pour une moyenne, on fait remonter somme + compte et on divise à la fin du reducer.

**6. B.** L'en-tête n'existe que dans le premier split : avec `next()`, le job réussit, `Map input records` reste à 954 et le résultat diffère d'une seule paire (1480 contre 1481).
**Pourquoi :** chaque mapper est un processus isolé qui ne lit que son split, donc `next()` ne filtre l'en-tête que chez le premier — les compteurs ne le révèlent pas, seule une valeur de contrôle attendue le montre.

**7. C.** Le nombre de mappers se lit dans le log du job, ligne `number of splits:` — sur notre cluster, un fichier d'un seul bloc donne 2 splits sans option.
**Pourquoi :** la taille du bloc ne fixe pas à elle seule le nombre de splits ; la configuration d'entrée pèse autant que la taille. La formule « taille / bloc » de la littérature est une approximation, pas une vérité.

**8.** Attendu : le mapper a planté sur un `int()` non protégé (ligne 576 du CSV, champ `streams` corrompu), Hadoop a relancé la tâche sans plus de succès, et le job est tombé. Le 473 est le compte du seul task survivant, pas la totalité des lignes.
**Pourquoi :** un compteur n'est fiable que si l'on sait quel task l'a monté — on ne lit jamais les compteurs d'un job en échec comme ceux d'un job réussi, et la preuve se cherche dans `yarn logs -applicationId …`.

</details>

---

[← Chapitre précédent](03-hdfs.md) · [Sommaire](Readme.md) · [Chapitre suivant →](05-hive.md)
