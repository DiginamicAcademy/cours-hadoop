# Corrigé — Exercices Hive (séquence 05)

> Les exercices concernés : [`../05-hive.md`](../05-hive.md) partie III + les questions de contrôle des parties I-II.

## Questions de contrôle (parties I-II)

**I.5 — Pourquoi 954 lignes dans le fichier et 954 dans la table ?**

Le fichier TSV contient le header (`track_name\tartist_name\t...`). Sans `TBLPROPERTIES ('skip.header.line.count'='1')`, Hive lit le header comme une ligne de données normale : `released_year` vaut `'released_year'` (pas un INT, donc `NULL` dans une colonne typée), et `COUNT(*)` renvoie **954** (953 chansons + 1 en-tête compté comme donnée). **Hive ne plante pas** — il ne lève aucune exception, il incorpore silencieusement la première ligne.

Deux solutions propres :

```sql
-- à la création de la table :
CREATE EXTERNAL TABLE spotify (...)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/root/spotify/clean'
TBLPROPERTIES ('skip.header.line.count'='1');
```

ou retirer le header côté fichier avant le `put` (le `wc -l` passerait alors à 953).

> **Note pour l'animation** : avec la DDL de l'EX 05 (sans `skip.header.line.count`), `COUNT(*)` vaut **954** (l'en-tête est compté). Avec `skip.header.line.count=1`, il vaut **953**. Les deux sont « corrects » — c'est le mécanisme qui importe. Un `COUNT(streams)` donne **952** dans les deux cas (la ligne 576 a `streams` non numérique → `NULL`). L'écart entre ces valeurs est le point pédagogique de contrôle.

**II.1 — Validation croisée avec MapReduce.** La requête équivalente à l'exercice 3 de l'EX 04 :

```sql
SELECT released_year, SUM(streams) AS total
FROM spotify
GROUP BY released_year
ORDER BY released_year;
```

Les chiffres doivent être **identiques** à la sortie du job MapReduce (le corrigé MR : `04-mapreduce.md`, exercice 1). S'ils divergent : vérifiez le header (cf. I.5), la ligne 576 du CSV (`streams` non numérique, passée à `NULL` par le `try/except int()` du mapper), et le type BIGINT de `streams`. Attention : la ligne 576 a bien 25 champs comme les autres — le script `../../annexes/utils/7` ne l'écarte pas, c'est le mapper qui la neutralise.

**II.5 — Lecture de l'EXPLAIN.** Sur un GROUP BY, le plan montre typiquement :

```
Stage-1 : Map Operator (lignes lues, agrégation partielle)
          → Reduce Operator (groupage par clé, SUM finale)
Stage-0 : Fetch (retour du résultat)
```

Le map = votre mapper Python, le reduce = votre reducer. Faites le parallèle oral avec les phases de l'EX 04 : c'est le moment où le lien MapReduce/Hive devient tangible.

## Partie III — Exercices autonomes

**Exercice 1 — Top 5 artistes par nombre de titres.**

```sql
SELECT artist_name, COUNT(*) AS nb_titres
FROM spotify
GROUP BY artist_name
ORDER BY nb_titres DESC
LIMIT 5;
```

> ⚠️ **Piège classique** : le champ `artist(s)_name` contient parfois plusieurs artistes séparés par des virgules (`"Latto, Jung Kook"`). Ici, on compte chaque ligne = un titre, groupée par le champ brut — les duos comptent comme un « artiste ». C'est une simplification assumée de l'exercice ; pour un comptage exact, il faudrait exploser les duos (fonction `split` + `explode` — hors périmètre, mentionnez-la en ouverture).

**Exercice 2 — Streams moyens par année.**

```sql
SELECT released_year, AVG(streams) AS moyenne_streams
FROM spotify
GROUP BY released_year
ORDER BY released_year;
```

Comparaison avec l'EX 04 ex.1 : la **somme** y était calculée par MapReduce ; la moyenne en découle (somme / nombre de titres de l'année). Vérification rapide : pour 2023, moyenne ≈ somme(2023) / nb_titres(2023).

**Exercice 3 — Hive ou MapReduce à la main ? (réponses attendues)**

- **(a) Exploration d'un dataset inconnu chaque semaine** → **Hive**. L'exploration ad-hoc est exactement le cas d'usage du SQL : question → requête, itération immédiate, aucune infrastructure à maintenir.
- **(b) Rapport nocturne avec SQL validé de 20 lignes** → **Hive**. Un SQL déjà validé se relance chaque nuit via `beeline -f rapport.sql` ; la maintenance est triviale comparée à 20 lignes de pipeline Python à recouvrir.
- **(c) Format binaire propriétaire + machine à états de 300 lignes** → **MapReduce à la main** (séquence 04). La logique métier complexe, au niveau enregistrement, ne s'exprime pas en SQL — c'est le territoire du code.

**Synthèse à faire émerger** : Hive gagne dès que la question est déclarative (filtrer, agréger, joindre) ; le code gagne dès que la transformation est procédurale et fine. L'EX 04 vous a appris le moteur ; Hive vous fait économiser de l'écrire quand le problème s'exprime en SQL.

---

## Pièges récurrents à anticiper (formateur)

1. **Header dans la table** : cf. I.5 — selon les promotions, la moitié de la classe aura 954 au COUNT. Faites chercher la cause avant de donner le `TBLPROPERTIES`.
2. **`key` mot réservé** : la colonne `key` du dataset exige des backticks — signalé dans l'EX 05, à rappeler à l'oral.
3. **Types** : `streams` en BIGINT (dépasse INT sur certains titres — cf. le même piège dans le corrigé MR 04), `in_shazam_charts` en STRING (valeurs non numériques / vides dans le dataset).
4. **Derby une session à la fois** : le Metastore embarqué (Derby, dans `/home/metastore_db`) n'accepte qu'une connexion à la fois — c'est le comportement normal de notre cluster mono-utilisateur. Si un apprenant double-connecte, message d'erreur de verrou : fermer l'autre session (`!quit`). Plusieurs sessions simultanées exigerait un vrai Metastore (MySQL) — hors périmètre.
5. **DROP sur MANAGED par erreur** : si un apprenant a créé une table interne (sans EXTERNAL) sur un dossier avec `LOCATION`, le DROP supprimerait les données — scenario rare ici (tables créées sans LOCATION déposeront dans l'entrepôt Hive, pas dans notre dossier), mais gardez le réflexe de le demander avant tout DROP.
