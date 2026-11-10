![Diginamic Logo](../assets/diginamic.png) ![Hadoop Logo](../assets/hadoop.png)

# **TP final : Analyse des ventes par ville — toute la pipeline**

> **Type** : TP projet de synthèse (évalué) — le seuil TP du cursus
> **Prérequis** : séquences 02 à 07 (environnement, HDFS, MapReduce, Hive, HBase, NiFi)
> **Durée estimée** : une journée (4 h à 5 h) — HDFS → MapReduce → Hive → HBase → NiFi → rendu
> **Données** : `../data/dataw_fro03_mini_1000.csv` (mini, 1 000 lignes) dans l'énoncé ; `../data/dataw_fro03.csv` (le fichier complet, ~135 000 lignes) en **bonus**
> **Contexte** : les EX 02 à 07 vous ont fait *appliquer chaque outil séparément* ; ce TP est l'exercice *intégrateur* et évalué — la même donnée traverse toute la chaîne, avec rapport et livrables.

C'est le **TP de synthèse** du cours : vous enchaînez toute la pipeline avec **une seule donnée**, puis vous rédigez la **documentation de vos actions et de leurs retours**. Chaque étape est une réponse à une question — gardez la trace de **ce que vous avez lancé** (la commande), **ce que vous avez observé** (le retour), et **ce que vous en concluez**.

_📌 Il n'est pas nécessaire de mettre tout le code dans le rapport, mais de bien expliquer étape par étape : le lecteur doit pouvoir reproduire vos actions sans votre aide._
_Exemple : utilisation d'une structure minimale._

> 🤔 **Question de contrôle** : notez en cours de route **où** chaque outil intervient dans la chaîne (qui ingère ? qui déplace ? qui stocke ? qui calcule ? qui sert ?). C'est exactement le plan du rapport demandé en livrable.

---

## **Sommaire**

- [I. Objectif — les questions du TP](#i-objectif--les-questions-du-tp)
- [II. HDFS — accueillir le fichier](#ii-hdfs--accueillir-le-fichier)
- [III. Question 1 — le classement des villes (MapReduce)](#iii-question-1--le-classement-des-villes-mapreduce)
- [IV. Question 2 — la clé composite ville#année (MapReduce)](#iv-question-2--la-clé-composite-villeannée-mapreduce)
- [V. Question 3 — le même résultat en SQL (Hive)](#v-question-3--le-même-résultat-en-sql-hive)
- [VI. Question 4 — stocker le résultat en clé/valeur (HBase)](#vi-question-4--stocker-le-résultat-en-clévaleur-hbase)
- [VII. Question 5 — ingérer au lieu d'un `put` (NiFi)](#vii-question-5--ingérer-au-lieu-dun-put-nifi)
- [VIII. Visualisation — le top 10 des villes](#viii-visualisation--le-top-10-des-villes)
- [Bilan — la chaîne complète](#bilan--la-chaîne-complète)
- [Bonus — le fichier complet](#bonus--le-fichier-complet)
- [Livrables attendus](#livrables-attendus)

---

## **I. Objectif — les questions du TP**

Les fichiers `dataw_fro03_*.csv` enregistrent des **commandes** d'articles (catalogue : « Tête de menagere », « Drap de bain », « Montre »…). Quatre colonnes vous intéressent (index de la colonne dans la ligne CSV, en partant de 0) :

| Colonne (index) | Nom | Usage |
|---|---|---|
| 5 | `villecli` | la ville du client |
| 7 | `datcde` | la date de commande (`AAAA-MM-JJ HH:MM:SS`, heure toujours à zéro) |
| 15 | `qte` | la quantité d'articles commandée |
| 17 | `libobj` | l'article commandé |

> ⚠️ **Attention** : dans ce jeu fourni, les colonnes de prix (`prixcond`, `puobj`) sont **à zéro** — on travaille donc sur les **quantités d'articles** (`qte`), pas sur les montants. Le cœur du TP (la chaîne technique) n'en change pas.

Vous répondez à **plusieurs questions**, dans l'ordre de la chaîne :

| # | Question | Outil | Chapitre d'appui |
|---|---|---|---|
| Q1 | Quelles sont les **villes** qui commandent le plus d'articles ? | MapReduce | [exercice 04](../exercices/04-mapreduce.md) (exercices 1-3) + [TD 05](../td/TD05.md) |
| Q2 | Même question, mais **par ville et par année** (clé composite) | MapReduce | [exercice 04](../exercices/04-mapreduce.md) (clé composite) |
| Q3 | Le **même classement en SQL** : les deux moteurs sont-ils d'accord ? | Hive | [exercice 05](../exercices/05-hive.md) + [TD 02](../td/TD02.md) |
| Q4 | Comment retrouver ces totaux **en clé/valeur** ? | HBase | [exercice 06](../exercices/06-hbase.md) + [TD 03](../td/TD03.md) |
| Q5 | Et si le fichier arrivait **ingéré par un flux** plutôt qu'un `put` ? | NiFi | [exercice 07](../exercices/07-nifi.md) + [TD 04](../td/TD04.md) |
| V | Le **graphique** du top 10 des villes | Matplotlib | [exercice 06](../exercices/06-hbase.md) (partie IV) |

Vous utiliserez **Hadoop MapReduce** pour calculer, **HappyBase** pour stocker, **Hive** pour valider en SQL, **NiFi** pour ingérer, et **Matplotlib** pour visualiser.

> **Aides** : scripts de référence dans [`../annexes/utils/`](../annexes/utils/) (`8-*` à `13-*`) ; énoncés des exercices dans [`../exercices/04-mapreduce.md`](../exercices/04-mapreduce.md) et suivants.

> ✨ **Principe** : chaque étape ci-dessous est celle d'un exercice déjà vu. **Rien de nouveau n'est à inventer : chaque commande a déjà été faite.** En cas de doute, le lien vers l'exercice d'origine est donné à côté.

---

## **II. HDFS — accueillir le fichier**

> Exercice d'origine : [`../exercices/03-hdfs.md`](../exercices/03-hdfs.md).

**1.** Déposez le mini-fichier dans le conteneur puis dans HDFS :

```bash
# depuis la machine Docker (pas le conteneur)
docker cp data/dataw_fro03_mini_1000.csv hadoop-master:/home/

# dans le conteneur hadoop-master
hdfs dfs -mkdir -p input
hdfs dfs -put dataw_fro03_mini_1000.csv input/
hdfs dfs -ls input
```

**Résultat attendu** : `input/dataw_fro03_mini_1000.csv` — `197743` octets, réplication `1`.

**2.** Vérifiez que le fichier est complet : `hdfs dfs -cat input/dataw_fro03_mini_1000.csv | tail -3` (la dernière ligne est une commande du client `4676` sur `SAINT ROMAIN DE COLBOSC`).

> 📋 **Pour le rapport** : documentez la connexion à la machine (IP, port, utilisateur — le mot de passe ne se documente pas) telle qu'elle a réellement fonctionné, les services démarrés et les commandes utilisées (HDFS, YARN, ZooKeeper, HBase). Chaque action du TP donne lieu à une ligne « action → retour observé ».

---

## **III. Question 1 — le classement des villes (MapReduce)**

> Exercices d'origine : [`../exercices/04-mapreduce.md`](../exercices/04-mapreduce.md) (prise en main et exercices 1-3).

**Q1 — Quelles villes commandent le plus d'articles (toutes années confondues) ?**

**1.** Reprenez les squelettes fournis au TP :

- Mapper : [`../annexes/utils/8-mapper-ventes.py`](../annexes/utils/8-mapper-ventes.py) — émet `ville<TAB>qte` (en-tête sauté par test de valeur, `int(qte)` protégé).
- Reducer : [`../annexes/utils/10-reducer-ventes.py`](../annexes/utils/10-reducer-ventes.py) — fait la somme par clé.

**2.** Testez **localement** avant le cluster :

```bash
cat data/dataw_fro03_mini_1000.csv | python3 annexes/utils/8-mapper-ventes.py | sort -t $'\t' -k1,1 | python3 annexes/utils/10-reducer-ventes.py | sort -t $'\t' -k2,2nr | head -5
```

**Résultat attendu** :

```text
LE MANS              30
MAMERS               18
DOMFRONT EN POIRAIE  16
ALENCON              15
DOUDEVILLE           15
```

(`569` villes au total, `999` lignes de données, **`1442` articles** toutes villes confondues.)

**3.** Lancez le job sur le cluster (services démarrés) :

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file annexes/utils/8-mapper-ventes.py -mapper "python3 8-mapper-ventes.py" \
  -file annexes/utils/10-reducer-ventes.py -reducer "python3 10-reducer-ventes.py" \
  -input input/dataw_fro03_mini_1000.csv -output output/ventes_ville
hdfs dfs -cat output/ventes_ville/part-00000 | sort -t $'\t' -k2,2nr | head -5
```

**Résultat attendu** : les mêmes valeurs que le test local (le cluster ne change *rien* au calcul, il le distribue).

> ⚠️ Rappel du chapitre 04 : la sortie ne doit **pas exister** avant le job (`-rm -r -f` si besoin).

> 📋 **Pour le rapport** : décrivez la structure d'un job MapReduce (mapper, reducer) et les commandes de soumission réelles, avec le **retour** observé (le `number of splits` du log, le nombre de mappers/reducers). Détaillez les protections du mapper (en-tête par valeur, `int()` protégé, ville vide) et **testez le comportement** d'une ligne parasite : que se passe-t-il si on laisse passer l'en-tête ?

---

## **IV. Question 2 — la clé composite ville#année (MapReduce)**

> Exercice d'origine : [`../exercices/04-mapreduce.md`](../exercices/04-mapreduce.md) — le geste de la clé composite.

**Q2 — Et en y regardant année par année ? Quelle ville·année commande le plus ?**

**1.** Utilisez le mapper de clé composite : [`../annexes/utils/9-mapper-ventes-annee.py`](../annexes/utils/9-mapper-ventes-annee.py) — émet `ville#année<TAB>qte`, en filtrant les dates invalides (`NULL` du fichier complet).

**2.** Testez localement, puis lancez le job :

```bash
cat data/dataw_fro03_mini_1000.csv | python3 annexes/utils/9-mapper-ventes-annee.py | sort -t $'\t' -k1,1 | python3 annexes/utils/10-reducer-ventes.py | sort -t $'\t' -k2,2nr | head -5
```

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file annexes/utils/9-mapper-ventes-annee.py -mapper "python3 9-mapper-ventes-annee.py" \
  -file annexes/utils/10-reducer-ventes.py -reducer "python3 10-reducer-ventes.py" \
  -input input/dataw_fro03_mini_1000.csv -output output/ventes_ville_annee
```

**Résultat attendu** (mini) :

```text
LE MANS#2006      30
MAMERS#2006       18
DOMFRONT EN POIRAIE#2006  16
LAVAL#2006        15
DOUDEVILLE#2006   15
```

`578` paires `ville#année` au total — le mini ne contient presque que l'année `2006` ; c'est sur le **fichier complet** (bonus) que cette question devient parlante.

> 💡 **Lien pédagogique** : la clé est **composite** — deux dimensions accolées dans une seule clé textuelle. Le reducer la décompose avant d'agréger : chaque dimension redevient un champ exploitable.

> 📋 **Pour le rapport** : expliquez pourquoi la clé composite passe par UNE seule clé (et le tri du shuffle), et notez ce que la question 2 apporte de plus que la question 1.

---

## **V. Question 3 — le même résultat en SQL (Hive)**

> Exercice d'origine : [`../exercices/05-hive.md`](../exercices/05-hive.md).

**Q3 — Le classement de Q1, mais en SQL : MR et Hive sont-ils d'accord ?**

**1.** Préparez le TSV (script [`../annexes/utils/12-prepare-ventes-tsv.py`](../annexes/utils/12-prepare-ventes-tsv.py)) — il retire l'en-tête et garde 4 colonnes `ville · annee · libobj · qte` :

```bash
python3 annexes/utils/12-prepare-ventes-tsv.py data/dataw_fro03_mini_1000.csv ventes-clean.tsv
wc -l ventes-clean.tsv          # 999 lignes de données (1 en-tête retiré)
hdfs dfs -mkdir -p /user/root/ventes/clean
hdfs dfs -put ventes-clean.tsv /user/root/ventes/clean/
```

**2.** Dans Hive (`./start-hive.sh`, `beeline -u jdbc:hive2://localhost:10000`), créez la table externe et interrogez :

```sql
CREATE DATABASE IF NOT EXISTS ventes_db;
CREATE EXTERNAL TABLE ventes_db.ventes (ville STRING, annee INT, libobj STRING, qte INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '/user/root/ventes/clean';

SELECT COUNT(*) FROM ventes_db.ventes;          -- 999
SELECT ville, SUM(qte) AS total FROM ventes_db.ventes
  GROUP BY ville ORDER BY total DESC LIMIT 5;   -- LE MANS 30 …
SELECT ville, annee, SUM(qte) AS total FROM ventes_db.ventes
  GROUP BY ville, annee ORDER BY total DESC LIMIT 5;  -- LE MANS # 2006 : 30 …
```

**Résultat attendu** : `COUNT(*)` = **999** ; le top 5 des villes est identique à Q1 ; le top ville·année est identique à Q2.

> ✨ **Lien pédagogique** : comparez le `part-00000` de Q1/Q2 et les deux dernières requêtes : **mêmes lignes** ? C'est la validation croisée du TD-02, refaite avec votre donnée.

> 📋 **Pour le rapport** : montrez la requête Hive qui reproduit l'agrégat de votre job, et commentez la comparaison (lignes de code, lisibilité, EXPLAIN). Tracez une commande `EXPLAIN SELECT …` et notez le retour dans le rapport.

---

## **VI. Question 4 — stocker le résultat en clé/valeur (HBase)**

> Exercice d'origine : [`../exercices/06-hbase.md`](../exercices/06-hbase.md) — partie IV (exercices 4-5).

**Q4 — Comment retrouver, en clé/valeur, le total d'articles d'une ville ?**

**1.** Démarrez HBase + Thrift et créez la table **avant** le job (jamais dans le reducer) :

```bash
./start-hbase.sh && ./start-thrift.sh
hbase shell
  > create 'ventes', 'stat'
  > exit
```

**2.** Relancez Q1 avec un reducer qui **écrit dans HBase** (rowkey = ville) : [`../annexes/utils/11-reducer-ventes-hbase.py`](../annexes/utils/11-reducer-ventes-hbase.py)

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file annexes/utils/8-mapper-ventes.py -mapper "python3 8-mapper-ventes.py" \
  -file annexes/utils/11-reducer-ventes-hbase.py -reducer "python3 11-reducer-ventes-hbase.py" \
  -input input/dataw_fro03_mini_1000.csv -output output/ventes_hbase
```

**3.** Vérifiez par `get` / `scan` (le `put` ne confirme rien) :

```bash
hbase shell
  > count 'ventes'           -- 569 villes sur le mini
  > get 'ventes','LE MANS'   -- stat:total => 30
```

**Résultat attendu** : `count` = **569** ; `get 'ventes','LE MANS'` affiche `stat:total      timestamp=…, value=30`. 30 = le total d'articles de LE MANS trouvé en Q1 : la cohérence entre les moteurs se vérifie.

> ⚠️ Le `11-reducer-ventes-hbase.py` n'écrit **rien sur stdout** (un `print` corromprait le `part-00000`) — les messages de suivi partent sur `stderr`.

> 📋 **Pour le rapport** : expliquez pourquoi ranger la **ville en rowkey** (et pas l'évènement de vente) sert la question, retrouvez la valeur de LE MANS par `get`, et commentez ce que le `count` vous apprend sur le volume de la table.

---

## **VII. Question 5 — ingérer au lieu d'un `put` (NiFi)**

> Exercice d'origine : [`../exercices/07-nifi.md`](../exercices/07-nifi.md).

**Q5 — Et si un flux déposait le fichier à notre place ?**

**1.** Déposez le mini-CSV dans le dossier d'entrée du flux :

```bash
cp data/dataw_fro03_mini_1000.csv nifi/input/
```

**2.** Dans NiFi (`https://localhost:8443/nifi`), vérifiez que le flux du chapitre 07 tourne : **GetFile → ConvertRecord → PutFile**, avec le câblage `failure` vers `/opt/nifi/errors`. Le fichier doit ressortir en **TSV** dans `/opt/nifi/output` (= `/home/staging` du master) — l'extension reste `.csv`, c'est le contenu qui change (cf. chap. 07).

**3.** Déposez le fichier produit dans HDFS, là où Hive le lit :

```bash
docker exec -it hadoop-master bash
hdfs dfs -moveFromLocal /home/staging/dataw_fro03_mini_1000.csv /user/root/ventes/clean/
```

**4.** Re-compter dans Hive pour prouver que l'ingestion a fonctionné :

```sql
SELECT COUNT(*) FROM ventes_db.ventes;   -- 1999 (999 + les 1000 lignes du flux)
SELECT ville, SUM(qte) AS total FROM ventes_db.ventes
  GROUP BY ville ORDER BY total DESC LIMIT 5;   -- LE MANS encore 30
```

**Résultat attendu** : le compteur **augmente** (+1 000 lignes, dont l'en-tête `ville` que le flux a conservé : la requête par ville reste correcte car la ligne d'en-tête a une année non numérique et des `qte` non numériques).

> 🎯 **Piège à observer** : le flux du chapitre 07 ré-écrit le fichier en TSV **avec son en-tête**, alors que le `12-prepare` l'avait retiré — le `COUNT(*)` vaut `1999`, pas `1998`. Repérez, dans **Data Provenance**, le lineage du fichier ingéré (GetFile → ConvertRecord → PutFile → …) : c'est la trace « qui, quand, quoi, où » demandée par ce TP.

> 📋 **Pour le rapport** : documentez l'alternative flux NiFi plutôt que le `put` manuel, ce que le flux ne fait pas tout seul (l'en-tête !), et qui surveille cette étape.

---

## **VIII. Visualisation — le top 10 des villes**

**1.** Créez le graphique avec Matplotlib : un script python **séparé** qui lit HBase (via HappyBase) et génère le PDF du top 10 des villes (référence : [`../annexes/utils/13-graph-ventes-hbase.py`](../annexes/utils/13-graph-ventes-hbase.py)).

```bash
python3 annexes/utils/13-graph-ventes-hbase.py > top10.txt
```

**Résultat attendu** : `LE MANS 30` en tête dans `top10.txt` et un `resultat.pdf` avec les 10 barres.

> ⚠️ Le script tourne **sur le master**, hors MapReduce, Thrift démarré (jarre du chapitre 06). `matplotlib.use('Agg')` avant l'import de `pyplot` est obligatoire sans écran.

**2.** Récupérez les sorties du job MapReduce et le PDF vers votre poste local :

```bash
hdfs dfs -get output/ventes_ville/part-00000 ./resultats.txt
docker cp hadoop-master:/home/resultat.pdf ./
docker cp hadoop-master:/home/resultats.txt ./
```

---

## **Bilan — la chaîne complète**

Vous venez d'enchaîner, avec **une seule donnée**, les cinq étapes de la pipeline :

| Étape | Outil | Rôle dans la chaîne | Vérification |
|---|---|---|---|
| Dépôt | HDFS | stocker le fichier (blocs, réplication 1) | `ls` / `cat \| tail` |
| Calcul | MapReduce | Q1 Q2 : compter les articles par ville / ville·année | `part-00000` |
| SQL | Hive | Q3 : même agrégat en SQL (validation croisée) | `COUNT(*)` = 999 |
| Clé/valeur | HBase | Q4 : totaux par ville (rowkey = ville) | `get` / `count` = 569 |
| Ingestion | NiFi | Q5 : déposer le fichier sans `put` manuel | `COUNT(*)` augmente |

```bash
# emplacement des sorties de cette session
output/ventes_ville/part-00000            # MapReduce Q1
output/ventes_ville_annee/part-00000      # MapReduce Q2
/user/root/ventes/clean/                # table externe Hive (LOCATION)
ventes                                    # table HBase (scan / get)
```
> ⚠️ La localisation Hive est celle définie dans `CREATE EXTERNAL TABLE … LOCATION` : reportez celle **que vous avez réellement utilisée**.

---

## **Bonus — le fichier complet**

Refaites **Q1, Q2 et Q3** sur le fichier complet `../data/dataw_fro03.csv` (~135 000 lignes de données) en réutilisant exactement les mêmes scripts — le pipeline **ne change pas**, c'est la taille qui change.

**Résultats attendus (complet)** :

- Q1 : `135 274` lignes retenues, total **`195 070` articles**, `6 255` villes distinctes — top : `LE MANS 3019 · CAEN 2056 · FLERS 1480 · LAVAL 1422`.
- Q2 : `31 241` paires ville#année — top : `LE MANS#2010 305 · BEAUFORT EN VALLEE#2012 305 · LE MANS#2006 267`.
- Q3 : en Hive, `WHERE annee IS NOT NULL` pour ignorer les deux lignes à date `NULL` (que le mapper 9 filtre aussi) — les top restent identiques.

> **Retours à noter** : temps de lancement, `number of splits` bien au-delà de 2, quelles villes apparaissent alors qu'elles n'étaient pas dans le mini… et ce que ça change (ou pas) à votre rapport.

---

## **Livrables attendus**

Le rendu est une **documentation des actions et de leurs retours**, enrichie des résultats observés :

1. **La documentation pas à pas** : pour chaque étape (Q1 → Q5), le **contexte**, la **commande lancée**, le **retour obtenu** (copie d'écran ou transcription), et la **conclusion** — y compris les erreurs rencontrées et corrigées.
2. **Le code source** de vos jobs MapReduce (ou le nom des scripts `../annexes/utils` réutilisés et pourquoi).
3. **Les résultats** : `part-00000` de Q1/Q2, requêtes Hive et comparaison MR ↔ Hive, `get`/`scan` HBase, et le `top10.txt`.
4. **Le graphique** Matplotlib (au format PDF).
5. **La matrice de contrôle** : pour chacun des cinq outils, **qui ingère ? qui déplace ? qui stocke ? qui calcule ? qui sert ?** — l'ossature du rapport, comme posée en question de contrôle.