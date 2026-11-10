# Corrigé — Exercices MapReduce (séquence 04)

> Les exercices concernés : [`../04-mapreduce.md`](../04-mapreduce.md) partie II (exercices 1 à 5).
> Batterie de données de référence (mesurée en session cluster du 11/09) : tout ce qui est en « Résultat attendu » ici a été reproduit au chiffre près — par un job réel, puis recoupé indépendamment en Python et en SQL (Hive).

## Exercice 1 — Total des streams par artiste

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

## Exercice 2 — Moyenne des streams par année de sortie

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

## [`../04-mapreduce.md`](../04-mapreduce.md) — Exercice 3 — Streams par année

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

## [`../04-mapreduce.md`](../04-mapreduce.md) — Exercice 4 — Moyenne de playlists par année

Même mapper avec `in_spotify_playlists` à la place de `streams`. Reducer « moyenne » : cf. exercice 2 de l'EX 04 (somme + compte, moyenne en fin de groupe). Vérification rapide : les moyennes doivent rester dans un ordre de grandeur plausible (quelques centaines de playlists), pas des milliards.

## [`../04-mapreduce.md`](../04-mapreduce.md) — Exercice 5 — Graphique PDF

Reducer : agrégation identique à l'exercice 1, puis génération matplotlib **à la fin du stdin** (toutes les clés vues) :

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

> **Piège** : `plt.savefig` doit être appelé **après** la boucle de lecture stdin, sinon le PDF est vide. Référence complète : [`../../annexes/utils/6-reducer-matplotlib-pdf.py`](../../annexes/utils/6-reducer-matplotlib-pdf.py).
