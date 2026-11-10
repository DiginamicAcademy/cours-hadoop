![Diginamic Logo](../assets/diginamic.png) ![Hadoop Logo](../assets/hadoop.png)

# Exercice 07 : NiFi

> **Type** : exercice guidé (parties I-II) + exercices autonomes (partie III)
> **Prérequis** : séquence 07 (NiFi), EX 05 (Hive) recommandé — on dépose les données de sa table externe
> **Durée estimée** : 1 h 30 (parties I-II) + 45 min à 1 h (partie III)
> **Lien cours** : [`../07 — NiFi, l'usine à flux de données`](../07-nifi.md)

Objectif de l'exercice : construire un **flux d'ingestion réel** — le CSV Spotify déposé dans un répertoire d'entrée est automatiquement transformé (CSV → TSV) puis écrit dans **HDFS**, à l'emplacement exact que lit la table externe Hive de l'EX 05. La même donnée, trois vues : **NiFi la déplace, Hive l'interroge, MapReduce la calcule**.

_📌 Pré-requis techniques : le conteneur NiFi (`hadoop-nifi`) est lancé avec le `docker compose up -d` de la partie 1 de l'EX 02. Si l'UI ne répond pas (`docker compose ps`), prévenez le formateur avant de continuer._

## Table des matières

- [Partie I — Prise en main](#partie-i--prise-en-main)
- [Partie II — Le flux Spotify → HDFS](#partie-ii--le-flux-spotify--hdfs)
- [Partie III — Exercices autonomes](#partie-iii--exercices-autonomes)

---

## Partie I — Prise en main

### 1. Ouvrir l'interface

Depuis votre navigateur : `https://localhost:8443/nifi` (en session locale ; `https://<IP_du_serveur>:8443/nifi` si le cluster est hébergé à distance — même principe que les interfaces NameNode et YARN de l'EX 02). NiFi 2.x impose le HTTPS avec un certificat auto-signé : au premier accès, le navigateur propose « accepter le risque » — acceptez une seule fois pour la session.

**Il reste une deuxième porte** : NiFi 2.x n'a plus de mode anonyme. Après le certificat, un **écran de login** s'affiche — connectez-vous avec le couple remis par le formateur (`etudiant` par défaut). Si ce couple est refusé, voyez la note ci-dessous avant de chercher plus loin.

**Résultat attendu** : le canvas NiFi s'ouvre, vide, avec la palette de processeurs en haut à gauche. Le badge en haut à droite affiche `etudiant@…` et aucun flux actif.

> ⚠️ **Le login est refusé / change tout seul ?** Le couple est fixé dans `nifi/credentials.env` du projet de déploiement. Sans ce fichier, l'image NiFi en **négocie un aléatoire** et l'écrit dans les logs — il change à chaque recréation du conteneur :
>
> ```bash
> docker logs hadoop-nifi | grep -A1 "Generated Username"
> ```
>
> La cause la plus fréquente est un **mot de passe de moins de 12 caractères** : NiFi le rejette silencieusement et retombe sur le couple aléatoire. Vérifiez la longueur, puis `docker compose up -d --force-recreate nifi`.

### 2. Premier processeur : GetFile

Glissez-déposez un processeur **GetFile** au centre du canvas :

- **Properties** :
  - `Input Directory` : `/opt/nifi/input` (le répertoire d'entrée du conteneur — c'est le montage du dossier `nifi/input/` du projet de déploiement)
  - `File Filter` : `.*\.csv`
  - `Keep Source File` : `false`
- **Scheduling** : `Run Schedule` = `5 sec`

> ⚠️ **Ne saisissez rien dans un champ `File Filter Regex`.** Le filtre qui existe sur GetFile s'appelle **`File Filter`**. « File Filter Regex » est une propriété *dynamique* : NiFi 2.11 l'accepte dans le formulaire mais n'a aucun validateur pour elle, et le processeur passe en **`Invalid`** avec, dans le bulletin :
>
> ```
> 'File Filter Regex' validated against '.*\.csv' is invalid because
> 'File Filter Regex' is not a supported property or has no Validator
> associated with it
> ```
>
> Un processeur `Invalid` **ne démarre pas** — le bouton Start refuse, sans message d'erreur dans le canvas. C'est l'un des deux ou trois cas où vous verrez NiFi refuser d'exécuter sans rien expliquer : prenez le réflexe d'ouvrir l'onglet *Information* (ou la cloche de bulletins) avant de chercher ailleurs.

- **Settings** : cochez `auto-terminate` sur **success**. Sans successeur **ni** auto-termination, NiFi marque le processeur `Invalid` et il ne démarre pas — un processeur isolé doit explicitement dire que sa sortie est une fin de course. (Dans un flux de production, cette case ne se coche qu'en dernier recours, jamais sur `failure` — voyez le II.)

Déposez un petit fichier `test.csv` (quelques lignes suffisent) dans `nifi/input/` (sur la machine hôte — c'est `/opt/nifi/input` vu du conteneur), puis **démarrez le processeur** (clic droit → Start, ou sélection + ▶).

**Résultat attendu** : sur le processeur, le compteur **out** (Data processed) passe de `0` à `1` — un FlowFile a été créé. Le fichier source a disparu du répertoire d'entrée.

> 🤔 **Question de contrôle** : où est passé le FlowFile ? Il n'y a pas de queue en sortie... _Réfléchissez : que signifie `auto-terminate` sur toutes les relations d'un processeur sans successeur ?_ (Réponse dans le corrigé — retenez que dans un flux de production, on câble `failure` au lieu de l'auto-terminer : nous le faisons au II.)

### 3. Data Provenance

Arrêtez le processeur. Ouvrez le menu global (coin haut droit) → **Data Provenance** (icône horloge/terrain) :

**Résultat attendu** : l'événement `RECEIVE` de votre `test.csv`, horodaté. Cliquez dessus → **l'onglet Lineage** (graphe des processeurs traversés), **Content** (le contenu du FlowFile — téléchargeable pour vérifier qu'il est intact).

C'est l'audit trail de votre ingestion : qui, quand, quoi, où — sans avoir écrit une ligne de code.

---

## Partie II — Le flux Spotify → HDFS

### 1. Poser les trois processeurs

Ajoutez au canvas (GetFile peut être réutilisé ou recréé) :

| Processeur | Rôle |
|---|---|
| **GetFile** | scrute `/opt/nifi/input`, capture le CSV Spotify |
| **ConvertRecord** | lit le CSV (séparateur `,`, quotes `"`), réécrit en TSV (séparateur `\t`) |
| **PutFile** | écrit le FlowFile dans `/opt/nifi/output` — le répertoire que **partagent** les conteneurs NiFi et master |

Câblez : GetFile `success` → ConvertRecord → PutFile `success`, et **chaque failure** vers un processeur PutFile vers `/opt/nifi/errors`.

> ⚠️ **C'est le moment 'fil rouge' de l'exercice** : une relation `failure` non câblée = une donnée susceptible de disparaître silencieusement. Aucune flèche ne doit rester en pointillé rouge dans le panneau des relations d'un processeur.

> ℹ️ **Pourquoi `PutFile` et pas `PutHDFS` ?** L'image `apache/nifi` que nous utilisons n'embarque **aucun bundle Hadoop**. Vérifiable depuis le conteneur — cette commande compte les bundles chargés par l'instance et y cherche Hadoop ; elle renvoie `0` :
>
> ```bash
> docker exec hadoop-nifi sh -c "ls /opt/nifi/nifi-current/work/nar/extensions/ | grep -ci hdfs"
> ```
>
> `PutHDFS` n'est donc **pas connu de l'instance** : si vous essayez de l'ajouter depuis la palette, NiFi refuse et le processeur reste introuvable à la recherche. Plutôt que de faire installer un NAR à chaque apprenant, le flux s'arrête à `PutFile` et **c'est le terminal qui pousse le fichier dans HDFS**, avec `hdfs dfs -moveFromLocal` — la même idée que `-put` de l'EX 05, à la différence que le fichier source est enlevé du conteneur. Le découpage est le même, il manque juste le dernier saut automatique — et vous le voyez, ce qui est plus formateur. Ajouter le bundle pour automatiser ce saut : voir *Pour aller plus loin*.

### 2. Configurer ConvertRecord

Dans les Properties de ConvertRecord, créez un controller service de lecture et un d'écriture (bouton **→** à droite de chaque propriété) — le pattern à retenir de NiFi : le processeur ne sait rien du format, ce sont les services qui le savent.

| Service | Propriété | Valeur | |
|---|---|---|---|
| **CSVReader** (le fichier source) | `Value Separator` | `,` | défaut : virgule |
| | `Quote Character` | `"` | |
| | `Treat First Line as Header` | **`true`** | ⚠ défaut : `false` |
| **CSVRecordSetWriter** (la sortie) | `Value Separator` | `\t` | ⚠ défaut : `,` |
| | `Include Header Line` | **`true`** | défaut : `true` |

Activez les deux controller services (bouton **Enable**) — ils sont partagés par tout le flux.

> ⚠️ **Les deux ⚠ ci-dessus ne sont pas décoratifs.** Par défaut, le reader **ne considère pas** la première ligne comme un en-tête : il la lit comme une chanson de plus, avec `released_year` = `'released_year'`. Le writer, lui, **écrit** bien un en-tête par défaut. Réglés comme dans le tableau, le fichier produit contient **une** ligne d'en-tête et **953** lignes de données :
>
> ```bash
> head -1 /home/staging/Spotify_Most_Streamed_Songs.csv   # un en-tete, pas une chanson
> wc -l   /home/staging/Spotify_Most_Streamed_Songs.csv   # 954 = 1 en-tete + 953 chansons
> ```
>
> Si vous lisez **955**, la première ligne du source a été comptée comme donnée *et* un en-tête a été réajouté ; si vous lisez **953**, il n'y a plus d'en-tête du tout. Ni l'un ni l'autre ne fait planter Hive : `COUNT(*)` compte juste faux, en silence — cf. piège n° 1 du corrigé 05.

> 💡 **Lien avec l'EX 05 (Hive).** Cette lecture CSV (quotes gérées) + réécriture TSV est la partie *mise en forme* de `../annexes/utils/7-prepare-spotify-tsv.py`. Sur ce jeu de données le résultat est **au même nombre de lignes** (954 : 1 en-tête + 953 chansons) — vérifiez-le, c'est une bonne maîtrise d'œuvre. Mais « même nombre de lignes » ne veut pas dire « mêmes données » : NiFi ne filtre rien, il convertit. Le script, lui, écarte toute ligne dont le nombre de champs n'est pas 25. Conservez le fichier produit par le flux : on le confronte aux sorties Hive (`COUNT` 953/954) et au TD 05 (provenance de la ligne 576, que **ni le script ni NiFi** ne signalent explicitement).

### 2bis. Configurer PutFile, puis pousser dans HDFS

**PutFile** — deux propriétés suffisent :

- `Directory` : `/opt/nifi/output`
- `Conflict Resolution Strategy` : **`replace`** (⚠ la valeur par défaut est `fail` : au deuxième dépôt d'un fichier de même nom, le FlowFile part en `failure` et rien n'est écrit. Réglez-le **avant** de démarrer.)

`Filename` peut rester à vide : PutFile utilise alors l'attribut `${filename}` du FlowFile, donc **le nom d'origine** — `Spotify_Most_Streamed_Songs.**csv**`, alors même que le contenu est devenu du TSV. Gardez-le tel quel : c'est un piège volontaire, voir la question de contrôle de la section 4. (Renommer en `.tsv` via un `UpdateAttribute` est l'exercice 2 de la partie III.)

Le répertoire `/opt/nifi/output` du conteneur NiFi et `/home/staging` du master sont **le même dossier** sur le disque. C'est ce qui permet de finir le travail dans le terminal, comme à l'EX 05 :

```bash
# depuis le master
docker exec -it hadoop-master bash
ls -l /home/staging                                   # le TSV vient d'apparaitre
hdfs dfs -moveFromLocal /home/staging/Spotify_Most_Streamed_Songs.csv \
                        /user/root/spotify/clean/
```

**Résultat attendu après démarrage du flux** :

1. Les compteurs in/out s'incrémentent sur chaque connection ;
2. `ls -l /home/staging` (master) montre le TSV juste produit ;
3. après le `moveFromLocal`, `hdfs dfs -ls /user/root/spotify/clean` affiche le fichier ;
4. `hdfs dfs -cat /user/root/spotify/clean/<fichier> | head -2` montre des **tabulations**, pas des virgules.

> 🤔 **Question de contrôle** : le flux s'arrête à `PutFile`, donc la dernière étape est humaine. Quel risque introduit ce saut manuel qu'un `PutHDFS` aurait câblé ? _Indice : qu'est-ce qui se passe si l'opérateur oublie la commande, ou la lance deux fois — et laquelle de ces deux erreurs est visible dans Data Provenance ?_

### 3. Interroger depuis Hive

**D'abord, regardez ce que contient le répertoire** — la table externe de l'EX 05 pointe sur `LOCATION '/user/root/spotify/clean'`, un *répertoire* : Hive lit **tous** les fichiers qui s'y trouvent.

```bash
hdfs dfs -ls /user/root/spotify/clean
```

- **Un seul fichier** (vous avez déplacé ou écrasé celui de l'EX 05) → la table lit le fichier que NiFi vient de produire.
- **Deux fichiers** (le `spotify-clean.tsv` de l'EX 05 *et* le vôtre) → vous êtes en train de compter deux fois la même chanson. C'est l'objet de la question de contrôle ci-dessous, ne truquez pas le résultat.

```sql
SELECT COUNT(*) FROM spotify_db.spotify;
```

**Résultat attendu, fichier unique** : **953** si la table porte `TBLPROPERTIES ('skip.header.line.count'='1')`, **954** sinon (l'en-tête est alors compté comme une chanson — cf. corrigé 05, piège n° 1). Dans les deux cas c'est bien le fichier *de NiFi* que Hive est en train de lire : la chaîne ingestion → stockage → interrogation est complète.

> 🤔 **Question de contrôle** : remettez le fichier de l'EX 05 aux côtés du vôtre et relancez le `COUNT(*)`. Obtenez-vous **exactement 1 906** (= 2 × 953) ? Si vous trouvez plus, décomposez avec `SELECT released_year, COUNT(*) … GROUP BY` pour identifier combien d'en-têtes ont été comptés comme des chansons — c'est la seule façon de savoir si `skip.header.line.count` s'applique **par fichier** ou **une fois par table**. Notez votre mesure, nous la comparons en groupe.

### 4. Étudier les échecs et la reprise

- Remettez `Conflict Resolution Strategy` de PutFile à **`fail`** (la valeur par défaut), puis déposez **deux fois de suite** le même CSV dans l'entrée. Le second dépôt ne produit rien de visible dans `/opt/nifi/output` — mais le FlowFile n'a pas disparu pour autant.
- Déposez un fichier volontairement corrompu (une ligne de texte quelconque renommée en `.csv`) dans l'entrée : il doit partir sur la relation `failure` et se retrouver dans `/opt/nifi/errors` — **pas disparaître**.
- Dans **Data Provenance**, retrouvez l'événement de ce fichier corrompu : le graphe de lineage montre le chemin `failure` — la preuve visuelle que la donnée n'est pas perdue.

> 🤔 **Question de contrôle** : avec `Conflict Resolution Strategy: fail`, que devient la queue entre ConvertRecord et PutFile si personne ne câble la relation `failure` ? _Indice : relisez la section back pressure du cours, puis regardez le compteur de la queue après plusieurs dépôts. Le flux ne tombe pas en erreur — il **s'arrête**, et c'est pire._

> ⚠️ **Et le nom, dans tout ça ?** Le fichier déposé s'appelle `…Songs.**csv**` alors que son contenu est du TSV. Rien ne casse ici, parce que Hive se fiche de l'extension — mais envoyez ce fichier à un outil qui se fie à l'extension, et il le lira en CSV. C'est ce que règle l'exercice 2.

---

## Partie III — Exercices autonomes

Les corrigés sont dans [`corriges/07-nifi.md`](corriges/07-nifi.md) — cherchez avant de regarder.

### Exercice 1 : Filtrage avant HDFS

Ne faire entrer dans le flux que les CSV de plus de 100 Ko — les petits fichiers de test ne doivent pas arriver jusqu'à HDFS.

Deux chemins, du plus simple au plus général :

1. **`GetFile` → `Minimum File Size` = `100 KB`.** Aucun nouveau processeur : la propriété existe déjà, et un fichier trop petit n'est jamais lu. Vérifiez-la dans vos Properties : elle est à `0 B` par défaut.
2. **`RouteOnAttribute`** entre GetFile et ConvertRecord, avec une expression NiFi sur l'attribut de taille — parce que dans un vrai flux, le filtre arrive souvent *après* une transformation, là où GetFile n'a plus son mot à dire.

> ⚠️ **Sur le chemin 2, n'écrivez pas le nom de l'attribut de mémoire.** Déposez un fichier, ouvrez sa fiche **Data Provenance → Attributes** (ou le menu *i* du FlowFile) et **recopiez** le nom exact de la taille. NiFi fait la différence entre `fileSize` et `filesize`, et une expression qui ne matche aucun attribut ne lève **aucune erreur** : tous les FlowFiles partent en `unmatched`, le flux a l'air de fonctionner, et rien n'avance. C'est le piège silencieux le plus fréquent de NiFi.

**Résultat attendu** : un petit fichier test déposé dans l'entrée n'arrive pas jusqu'à `/opt/nifi/output` ; le CSV Spotify (plus lourd) y arrive. Si **rien** n'arrive, ouvrez la fiche du processeur de routage et regardez sur quelle relation sont partis vos FlowFiles.

### Exercice 2 : Renommer et dater l'ingestion

Insérez un **UpdateAttribute** entre ConvertRecord et PutFile, pour obtenir un nom de fichier qui dit la vérité sur deux points : le **format** (le contenu est du TSV, le nom dit `.csv` — voir l'encadré de la section II.4) et la **date d'ingestion**.

Un `Filename` de la forme `spotify_${date:format('yyyy-MM-dd')}.tsv` suffit. Les expressions NiFi (`${now():toNumber():format(...)}`, `${date:format(...)}`) sont dans l'aide contextuelle du champ.

**Résultat attendu** : `/opt/nifi/output` contient un fichier au nom daté et en `.tsv` ; l'attribut est visible dans la fiche **Data Provenance → Attributes** du FlowFile. Si le fichier écrit garde l'ancien nom, vérifiez que PutFile n'a pas `Filename` figé à une valeur littérale.

### Exercice 3 : Pourquoi NiFi plutôt qu'un cron + hdfs dfs -put ?

Question de réflexion (3-5 lignes dans votre rapport de TP) : votre collègue propose de remplacer tout votre flux NiFi par un cron `*/5 * * * * hdfs dfs -put /input/*.csv /user/root/spotify/clean` et un petit script Python pour le TSV. Quels arguments de data engineer opposez-vous ?

**Résultat attendu** : au moins trois des cinq arguments — **traçabilité** (provenance, audit), **reprise sur erreur** (failure câblée, replay), **back pressure** (pas de submersion), **visibilité opérationnelle** (compteurs, badges, UI), **cycle de vie** (modifier un flux à chaud vs redéployer un script).

---

## Pour aller plus loin

- **Automatiser le dernier saut** : récupérer le bundle `nifi-hadoop-processors` compilé pour NiFi 2.11 et le déposer dans `nifi/nar_extensions/` du projet de déploiement — c'est le répertoire que l'image charge à chaud (`nifi.nar.library.autoload.directory=/opt/nifi/nifi-current/nar_extensions`, vérifiable dans `conf/nifi.properties`). Après redémarrage du conteneur, `PutHDFS` devient disponible, avec `Hadoop Configuration File Resources` pointé sur `/opt/nifi/hadoop/core-site.xml` (déjà monté). **C'est une dépendance externe au cours** — un NAR mal compilé peut empêcher NiFi de démarrer : à valider hors groupe avant de le montrer.
- **Paramétrer le flux** (Parameter Contexts) : externaliser le chemin HDFS et le répertoire d'entrée en paramètres pour passer d'un environnement à l'autre sans éditer les processeurs.
- **Registre de flux** (Flow Registry) : versionner le flux comme du code — hors périmètre du cours, mais mentionné dans le cours 07.
- **Lien TP final** : votre projet de synthèse peut ouvrir sur une ingestion NiFi du dataset — c'est un argument de rapport.

---

## Annexe — Piloter le même flux par l'API REST (notes de déploiement)

Le flux des parties I-II se construit aussi par requêtes HTTP sur `https://localhost:8443/nifi-api` — utile pour provisionner le canvas sans cliquer, ou pour reproduire l'exercice sur un cluster de réserve. Retours d'expérience du déploiement de référence (NiFi 2.11) :

- **Auth** : l'API n'accepte pas le couple Basic (`-u`). Il faut d'abord demander un jeton, puis le passer en `Authorization: Bearer` :
  `curl -k -X POST -H 'Content-Type: application/x-www-form-urlencoded' -d 'username=etudiant&password=…' https://localhost:8443/nifi-api/access/token`
- **Noms de propriétés** : dans le JSON `PUT /processors/{id}`, les propriétés s'écrivent **avec les espaces de l'UI** — `Record Reader`, `Record Writer`, `Include Zero Record FlowFiles` (attention à la casse : `FlowFiles`, pas `Flowfiles`). Le nom « canonique » vu nulle part dans le canvas (ex. `record-reader`) ne matche aucun validateur.
- **Mise à jour d'une config processor** : le `PUT` **fusionne** les propriétés — un champ à `""` vide **ne supprime pas** la valeur précédente ; pour vider une propriété boguée, il faut transmettre du `null` JSON sur cette clé.
- **Suppression d'une connexion** : elle exige la **queue vidée** et les processeurs **STOPPED**, sinon l'API répond `409`.
- **Scheduling** : NiFi 2.11 n'a **plus de mode `EVENT_DRIVEN`** (déprécié) — ne l'utilisez jamais dans un script REST, ce processeur restera `DISABLED` ou passera `Invalid`. Utilisez `TIMER_DRIVEN` (5 s), comme dans l'UI.
- **Provenance** : la requête minimale `POST /nifi-api/provenance` avec `{"provenance":{"request":{"maxResults":N}}}` suffit ; certains serveurs rejettent les `/startDate` au format `MM/dd/yyyy HH:mm:ss.fff` par un `Message body is malformed` — laissez-les de côté, le tri par horodatage suffit.

> Ces points sont intégrés au flux de référence du déploiement : scripts PowerShell `nifi-*.ps1` du dossier `VALIDATION/` du dépôt [Hadoop](https://github.com/robinhotton/Hadoop) (jouables contre l'API).
