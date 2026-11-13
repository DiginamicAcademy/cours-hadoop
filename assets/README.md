# Le dossier `assets`

Ce dossier rassemble ce que le cours montre sans l'écrire : logos, captures
d'écran, schémas exportés, illustrations. Le Readme, les chapitres et
`slides.md` y puisent ; GitHub, le support PDF et les diapositives les
affichent.

## Ce qu'il contient

| Fichier | Fond | Usage |
|---|---|---|
| `Logo-diginamic-color.svg` | clair | en-tête du Readme et du support PDF ; même logo que celui des diapositives claires |
| `Logo-diginamic-color-blk.png` | sombre | en-tête du Readme quand GitHub est en thème sombre ; même logo que le pied de page des diapositives |


Captures et schémas du cours, et où ils servent :

| Fichier | Référencé dans |
|---|---|
| `70percent.png` | `01-introduction-hadoop.md`, `slides.md` |
| `8088.jpeg` | `02-presentation-env-hadoop.md`, `exercices/02-lancement-de-hadoop.md`, `slides.md` |
| `9870.jpeg` | `02-presentation-env-hadoop.md`, `exercices/02-lancement-de-hadoop.md`, `slides.md` |
| `chain-complete.svg` | `07-nifi.md`, `slides.md` |
| `cluster-docker.svg` | `02-presentation-env-hadoop.md`, `07-nifi.md`, `slides.md` |
| `cluster.jpeg` | `01-introduction-hadoop.md`, `slides.md` |
| `diginamic.png` | `01-introduction-hadoop.md`, `02-presentation-env-hadoop.md`, `04-mapreduce-et-yarn.md`, `05-hive.md`, `06-hbase-et-zookeeper.md`, `07-nifi.md`, `A-Glossaire-et-ressources.md`, `exercices/02-lancement-de-hadoop.md`, `exercices/04-mapreduce.md`, `exercices/05-hive.md`, `exercices/06-hbase.md`, `exercices/07-nifi.md`, `td/TD01.md`, `td/TD02.md`, `td/TD03.md`, `td/TD04.md`, `td/TD05.md`, `tp/TP-01-enonce.md` |
| `ecosystem-hadoop.jpeg` | `01-introduction-hadoop.md`, `slides.md` |
| `fair-scheduler.jpeg` | `slides.md` |
| `hadoop-noeuds.svg` | `01-introduction-hadoop.md`, `slides.md` |
| `hadoop.png` | `01-introduction-hadoop.md`, `02-presentation-env-hadoop.md`, `04-mapreduce-et-yarn.md`, `05-hive.md`, `06-hbase-et-zookeeper.md`, `07-nifi.md`, `A-Glossaire-et-ressources.md`, `exercices/02-lancement-de-hadoop.md`, `exercices/04-mapreduce.md`, `exercices/05-hive.md`, `exercices/07-nifi.md`, `td/TD01.md`, `td/TD02.md`, `td/TD04.md`, `td/TD05.md`, `tp/TP-01-enonce.md` |
| `happybase.png` | `06-hbase-et-zookeeper.md`, `td/TD03.md` |
| `hbase-cluster.jpeg` | `06-hbase-et-zookeeper.md` |
| `hbase-rdbms-compare.jpeg` | `06-hbase-et-zookeeper.md`, `slides.md` |
| `hbase-row-compo.jpeg` | `06-hbase-et-zookeeper.md` |
| `hbase-schema-stockage.png` | `06-hbase-et-zookeeper.md`, `slides.md` |
| `hbase-shared-explain.jpeg` | `06-hbase-et-zookeeper.md` |
| `hbase-shell.png` | `06-hbase-et-zookeeper.md`, `exercices/06-hbase.md` |
| `hbase-table-model.jpeg` | `06-hbase-et-zookeeper.md`, `slides.md` |
| `hbase.jpeg` | `exercices/06-hbase.md`, `td/TD03.md` |
| `hdfs-architecture.svg` | `03-hdfs.md` |
| `hdfs-checkpoint.svg` | `03-hdfs.md`, `slides.md` |
| `hdfs-lecture-ecriture.svg` | `03-hdfs.md`, `slides.md` |
| `hdfs-principe.svg` | `03-hdfs.md`, `slides.md` |
| `hive-architecture.svg` | `05-hive.md`, `slides.md` |
| `logs-trajet.svg` | `slides.md` |
| `mapreduce-pipeline.svg` | `04-mapreduce-et-yarn.md`, `slides.md` |
| `mapreduce_sort.png` | `04-mapreduce-et-yarn.md`, `exercices/04-mapreduce.md` |
| `mapreduce_unsort.png` | `04-mapreduce-et-yarn.md` |
| `mr-process.jpeg` | `04-mapreduce-et-yarn.md`, `exercices/04-mapreduce.md` |
| `mr-stats.jpeg` | `04-mapreduce-et-yarn.md`, `exercices/04-mapreduce.md` |
| `mr-word-count-2.png` | `04-mapreduce-et-yarn.md`, `slides.md` |
| `mr.png` | `04-mapreduce-et-yarn.md` |
| `nifi-flux-ex07.svg` | `07-nifi.md` |
| `nifi-relationships.svg` | `07-nifi.md` |
| `politiques-ordonnancement.svg` | `04-mapreduce-et-yarn.md` |
| `resourcemanger.jpeg` | `04-mapreduce-et-yarn.md` |
| `scheduler.png` | `slides.md` |
| `script-happybase-1.png` | `06-hbase-et-zookeeper.md`, `td/TD03.md` |
| `script-happybase-2.png` | `06-hbase-et-zookeeper.md`, `td/TD03.md` |
| `skills.jpeg` | `01-introduction-hadoop.md`, `slides.md` |
| `sps.jpeg` | `01-introduction-hadoop.md` |
| `stockage-distribue.svg` | `slides.md` |
| `start-thrift.png` | `06-hbase-et-zookeeper.md` |
| `tail.jpeg` | `03-hdfs.md` |
| `taille-bloc.svg` | `slides.md` |
| `yarn-config.png` | `04-mapreduce-et-yarn.md`, `slides.md` |

Les schémas en SVG sont dessinés à la maison, sur le cluster du cours et dans sa
langue : `hdfs-principe`, `hdfs-architecture`, `hdfs-checkpoint`,
`hdfs-lecture-ecriture`, `cluster-docker`, `mapreduce-pipeline`, `logs-trajet`,
`hive-architecture`, `nifi-relationships`, `nifi-flux-ex07`, `chain-complete`,
`hadoop-noeuds`, `taille-bloc`, `politiques-ordonnancement`. Les captures
d'écran (`9870`, `8088`, `tail`, `yarn-config`, `hbase-shell`, `start-thrift`,
`script-happybase-*`) gardent leur nature : elles prouvent un état réel.

Vingt-deux visuels ont été retirés le 12/11/2026. Dix-huit n'étaient référencés
par aucun document — figures héritées des versions d'avant, dont
`fsimages-edit.jpeg` et `mr-word-count.jpeg`, depuis remplacées par des SVG
maison. Quatre autres ont été remplacés par un schéma dessiné ici :
`namenode_datanode.png` → `hadoop-noeuds.svg`, `block-size.jpeg` →
`taille-bloc.svg`, `scheduling-policies.jpeg` → `politiques-ordonnancement.svg`,
et `working-hdfs.jpeg`, dont le contenu faisait doublon avec `hdfs-principe.svg`.

Les deux logos sont des copies du dépôt
[PublicAssets](https://github.com/DiginamicInternal/PublicAssets). Le texte de
la version `-blk`, gris très clair, disparaît sur fond blanc : c'est pourquoi le
Readme les déclare toutes les deux dans une balise `<picture>`. GitHub choisit
selon le thème du lecteur ; pour le PDF, imprimé sur fond blanc, le workflow ne
garde que la version couleur.

> Merci de ne pas modifier les logos 😉

## Y ranger ses images

Une image se désigne par son chemin relatif à la racine du dépôt :

```markdown
![Architecture de la réplication](assets/replication.png)
```

Tous les fichiers du cours vivent à la racine : le même chemin vaut dans le
Readme, dans un chapitre et dans `slides.md`. Des sous-dossiers sont possibles
(`assets/chapitre-02/…`) pour ranger un cours chargé.

Quelques règles évitent les mauvaises surprises :

- **pas d'espace ni d'accent** dans les noms : un espace oblige à l'encoder
  (`%20`) dans le lien ;
- **la casse compte** : le workflow tourne sous Linux, où `Schema.png` et
  `schema.png` sont deux fichiers différents. Windows et macOS tolèrent l'écart,
  la construction du PDF non ;
- le **SVG** pour les schémas, le **PNG** pour les captures d'écran ;
- une **sortie de terminal** n'est pas une image : c'est du texte, et elle se
  recopie dans un bloc de code — elle devient copiable et cherchable ;
- une **capture d'interface réelle** reste une image : la redessiner en SVG
  produirait un faux écran.

## Ce qu'en fait le workflow

- **Support de cours** : chaque image locale citée est recopiée à côté du
  document avant le rendu. Une image introuvable fait échouer la construction,
  et le journal donne le chemin en cause. Une image citée dans un bloc de code
  ou entre accents graves est un exemple : elle n'est pas recherchée.
- **Diapositives** : Marp lit les images du dépôt directement, mais sans
  garde-fou. Une image introuvable y laisse un cadre vide sans faire échouer la
  construction.

Ce fichier-ci n'apparaît pas dans le support : seuls les `.md` de la racine y
sont repris.
