# Corrigé — Exercices NiFi (séquence 07)

> Les exercices concernés : [`../07-nifi.md`](../07-nifi.md) partie III + questions de contrôle des parties I-II.

## Questions de contrôle (parties I-II)

**I.2 — Où est passé le FlowFile ?**

`auto-terminate` signifie : _« le processeur considère son travail terminé et **supprime le FlowFile** après émission »_ — sans queue de sortie, l'événement est consigné dans la provenance mais la donnée s'arrête là. C'est exactement pourquoi un flux de production n'active **jamais** cette case sur `failure` sans l'avoir câblée : auto-terminer un échec = perdre la trace de la donnée.

Attention au piège symétrique, et il est plus fréquent que le précédent : **ne pas** cocher `auto-terminate` sur une relation sans successeur rend le processeur `Invalid` et le bouton Start refuse, sans rien afficher sur le canvas. Message relevé en préparant l'EX (sur PutFile, même mécanisme pour GetFile isolé) :

```
'Relationship success' is invalid because Relationship 'success' is not
connected to any component and is not auto-terminated
```

Une relation doit donc **toujours** avoir un successeur ou être auto-terminée : il n'y a pas de troisième choix.

> Cas fréquent de « ça ne démarre pas et NiFi ne dit rien » : une propriété **dynamique** mal nommée — par exemple `File Filter Regex` sur GetFile (la vraie propriété est `File Filter`). NiFi l'accepte dans le formulaire, marque le processeur `Invalid`, et le bouton Start refuse sans explication dans le canvas. Le message est dans l'onglet *Information* / les bulletins.

**II.2bis — Le saut manuel `PutFile` → HDFS**

Un `PutHDFS` câblé échoue **devant** NiFi et le consigne ; l'opérateur qui oublie le `moveFromLocal` ne laisse **aucune trace** — le fichier dort dans `/home/staging` et la table Hive reste silencieusement à J-1. Le `moveFromLocal` lancé deux fois, lui, est visible (la seconde échoue, `No such file or directory`). C'est la réponse attendue : le risque n'est pas la panne, c'est **l'oubli silencieux**. Mitigations à faire trouver : surveiller la taille de `/home/staging`, ou cron + verrou, ou le bundle NAR (voir « Pour aller plus loin »).

**II.4 — Conflict Resolution Strategy: fail**

Le second dépôt du même nom échoue sur PutFile (`fail` est la valeur **par défaut**), le FlowFile part sur `failure`. **Mais** observez la queue entre ConvertRecord et PutFile : l'échec répété + un `Run Schedule` de 5 sec la font grossir — et quand le *back pressure object threshold* est atteint, NiFi **arrête le flux en amont** plutôt que de laisser déborder : GetFile est stoppé. Aucun message d'erreur rouge, le flux est simplement figé. À montrer en live si le temps le permet — c'est la démonstration que la back pressure n'est pas un garde-fou optionnel mais le mécanisme de survie du flux.

## Partie III — Exercices autonomes

**Exercice 1 — Filtrage par taille.**

Chemin le plus court : `GetFile` → `Minimum File Size` = `100 KB`. Le fichier trop petit n'est jamais lu ; aucune logique de routage à écrire.

Chemin « production » : insérer un **RouteOnAttribute** entre GetFile et ConvertRecord.

- `Routing Strategy` : `Route to Property name` (c'est déjà la valeur par défaut — la vérifier ne coûte rien)
- Propriété dynamique : `big-file` = `${<attribut-de-taille>:gt(102400)}` — où `<attribut-de-taille>` est **recopié** depuis la fiche *Attributes* d'un FlowFile déposé
- Câbler `big-file` → ConvertRecord, et `unmatched` → auto-terminate (ou funnel).

**Le point à surveiller, et à faire chercher avant de le donner** : le nom de l'attribut de taille. Une expression qui ne résout aucun attribut ne lève **aucune erreur** — le test est faux, donc tous les FlowFiles partent en `unmatched`, et le flux paraît vivant alors qu'il ne traite plus rien. D'où la consigne de l'énoncé : recopier le nom depuis la fiche *Attributes* d'un FlowFile, jamais de mémoire. (Nous-mêmes n'avons pas tranché la graphie `fileSize`/`filesize` en 2.11 depuis les seuls fichiers du cours — c'est précisément la raison de la consigne.)

Alternative valable : **RouteOnContent**, ou UpdateAttribute + RouteOnAttribute en deux étapes — l'important est la **décision de routage sur attribut**, pas la solution exacte.

**Exercice 2 — Renommer et dater l'ingestion.**

**UpdateAttribute** entre ConvertRecord et PutFile :

- `ingestion_date` = `${now():format("yyyy-MM-dd'T'HHmm")}`
- `Filename` (propriété de PutFile) = `spotify_${ingestion_date}.tsv`, ou laisser `Filename` vide et régler l'attribut `filename` dans UpdateAttribute

Le fichier écrit porte alors l'horodatage **et** l'extension du contenu réel. Vérification : fiche Data Provenance → Attributes montre `ingestion_date` ; `ls /opt/nifi/output` montre le nouveau nom. Si le nom ne change pas, c'est que `Filename` de PutFile est figé à une valeur littérale — PutFile ne suit l'attribut `filename` que si sa propriété `Filename` est vide ou en EL.

**Variante pour aller plus loin** : un UpdateRecord peut écrire l'horodatage **dans le contenu** (colonne supplémentaire) — distinguer attribut (métadonnée de transport) et contenu (donnée) : bon débat de fin de séance.

**Exercice 3 — Pourquoi NiFi plutôt qu'un cron + put ? (réponses attendues)**

- **Traçabilité** : Data Provenance donne l'audit complet (qui/quoi/quand/où) — le cron ne trace que ses logs shell, sans lien avec la donnée.
- **Reprise sur erreur** : une `failure` câblée + replay point à point ; le cron échoue en silence (fichier restant dans /input, écrasements possibles).
- **Back pressure** : si HDFS ralentit, le flux s'arrête proprement en amont ; le cron continuerait de déposer (et de surcharger).
- **Visibilité opérationnelle** : compteurs temps réel, badges d'état, UI partagée ; le cron se supervise via crontab + grep dans les logs.
- **Cycle de vie** : modifier un flux = 2 minutes à la souris à chaud ; modifier le script = redéploiement, test, versionnage manuel.

**Synthèse à faire émerger** : le cron répond _« déplacer un fichier toutes les 5 minutes »_ ; NiFi répond _« exploiter une ingestion de données »_ — avec l'audit et la reprise comme différenciateurs, exactement ce que réclame la production (SLA, incidents, audit).

---

## Pièges récurrents à anticiper (formateur)

1. **Controller services non activés** : après création du CSVReader/Writer, il faut les **Enable** — sinon ConvertRecord reste en erreur au démarrage (badge rouge). Premier réflexe de classe à couvrir.
2. **Processeur `Invalid` sans message** : le canvas n'explique pas pourquoi Start refuse. Ouvrir l'onglet *Information* du processeur ou la cloche à bulletins. Deux causes reviennent : propriété dynamique mal nommée (`File Filter Regex` au lieu de `File Filter`) et relation non câblée non auto-terminée (`'Relationship success' is invalid because … not connected to any component and is not auto-terminated`).
3. **Deux fichiers dans le `LOCATION`** : la table Hive de l'EX 05 lit un **répertoire**. Si `spotify-clean.tsv` (EX 05) et le fichier de NiFi coexistent dans `/user/root/spotify/clean`, le `COUNT(*)` compte les deux — et `skip.header.line.count` est un nombre de lignes **par fichier**, donc l'en-tête du second peut passer. Faire mesurer (question de contrôle II.3) plutôt que corriger.
4. **En-tête : les deux défauts jouent l'un contre l'autre** — `Treat First Line as Header` est à `false` sur le reader (la 1ʳᵉ ligne du source devient une chanson) alors que `Include Header Line` est à `true` sur le writer (un en-tête est quand même écrit). Bien régler les deux, sinon `wc -l` donne 955 ou 953 au lieu de 954.
5. **GetFile garde ou supprime la source** : `Keep Source File = false` supprime le fichier d'entrée après capture — si on redémarre le flux pour une démo, le CSV est déjà parti. Prévoir une copie de secours **hors de** `nifi/input/`.
6. **`Conflict Resolution Strategy` par défaut = `fail`** sur PutFile : au deuxième dépôt du même nom, plus rien n'est écrit dans `output` et le FlowFile s'accumule côté `failure`. Régler sur `replace` à l'EX, et s'en servir pour la démo back pressure (II.4).
7. **Back pressure surprise** : avec un `Run Schedule` court et une `failure` non câblée, la queue rouge apparaît vite en démo. C'est une fonctionnalité à montrer, pas un bug.
8. **Le dépôt manuel** : `/home/staging` (master) = `/opt/nifi/output` (NiFi). Oublier le `moveFromLocal` ne lève **aucune** erreur et Hive reste silencieux à J-1 — surveiller la taille du répertoire, ou l'ajouter au rituel de fin d'exercice.
