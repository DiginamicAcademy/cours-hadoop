# TP-01 — Analyse des streams par artiste (projet de synthèse)

**Énoncé complet → [`TP-01-enonce.md`](TP-01-enonce.md).** C'est le document de travail de l'apprenant : le contexte, les questions du TP, les étapes de la chaîne, ce qu'il faut rendre et le modèle de rapport attendu.

## En résumé

Un **projet intégrateur** au sens strict : tout le cours (HDFS → MapReduce → Hive → HBase → NiFi), une donnée, de bout en bout, un livrable. La donnée est celle du **fil rouge** — le CSV Spotify (`data/Spotify_Most_Streamed_Songs.csv`, 953 chansons, 644 artistes) — et les **questions sont inédites** : classement des artistes par streams cumulés, clé composite `artiste#année`, rowkey HBase sur l'artiste, ingestion NiFi, graphique du top 10. **Durée : une journée (4 h à 5 h)** — c'est le jour 5 du cours. Il est **évalué** : c'est le seuil TP du cursus.

**Bonus — changement de dimension** : le top des **titres**, et non plus celui des artistes — la même chaîne réutilisée sans être réécrite, pour une question différente.

**Ce qui est évalué, c'est la chaîne autant que le rapport** : pour chaque étape, la commande lancée, ce qui a été observé, ce qu'on en conclut. Le lecteur doit pouvoir reproduire vos actions sans votre aide — le code intégral n'est pas requis, le raisonnement si.

## Ce qui vit où

| Contenu | Emplacement |
|---|---|
| Énoncé (contexte, questions, étapes, consignes de rendu) | **public — `tp/TP-01-enonce.md`** |
| Grille d'évaluation et éléments de corrigé | **privé `-Peda`** (`evaluations/`, `corriges/` — à produire) |

---

*Le TD 05 (`../td/TD05.md`) sert de pré-échauffement : il rejoue la chaîne en local sur la même donnée, sans rien de nouveau à inventer.*
