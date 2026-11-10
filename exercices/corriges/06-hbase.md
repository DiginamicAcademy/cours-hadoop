# Corrigé — Exercices HBase (séquence 06)

> Les exercices concernés : [`../06-hbase.md`](../06-hbase.md) parties III-IV.

## Partie III — hbase shell

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

## Partie III — HappyBase Python

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

## Partie IV — Spotify exercices 4-5

**Exercice 4 — insertion HBase des streams par année.** Mapper identique au corrigé Spotify 1 ([`04-mapreduce.md`](04-mapreduce.md)). Reducer : cf. [`../../annexes/utils/5-reducer-hbase.py`](../../annexes/utils/5-reducer-hbase.py) — le squelette crée `streams_year` (ou `streams_artist`) et insère chaque agrégat avec `table.put()`. La row key est la clé du stdin (année ou artiste), pas un index. Pour la variante « par année », utiliser `streams_year` comme nom de table et l'année comme row key :

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

**Exercice 5 — streams par artiste + PDF.** Structure : mapper (artiste, streams) → reducer agrégé qui insère en HBase (même squelette que ci-dessus, row key = artiste) → **script séparé** qui relit HBase et exporte le graphique :

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
