# Répartition des rôles et fichiers à maîtriser

Chaque étudiant est responsable d'une couche et doit pouvoir l'expliquer à l'oral, fichier par fichier.
Tout le monde doit aussi comprendre l'architecture complète (voir [architecture.md](architecture.md)).

## Étudiant 1 : Infrastructure et Ingestion

**Rôle :** cluster Docker reproductible, flux CSV → Kafka.

| Fichier | À savoir expliquer |
|---|---|
| `compose.yaml` | Les 11 services, leurs rôles, les profils (`tools`, `streaming`, `batch`), les volumes persistants (un par DataNode), les ports exposés |
| `infra/hadoop/hadoop.env` | `fs.defaultFS`, **`dfs.replication=2`** (exigence de la fiche), pourquoi les permissions HDFS sont désactivées en démo |
| `infra/spark/Dockerfile` | Pourquoi une image unique (même Python sur driver et executors), les connecteurs Kafka et PostgreSQL embarqués |
| `infra/spark/spark-defaults.conf` | `spark.master` (tous les jobs passent par le cluster), réplication côté client HDFS, mémoire limitée |
| `infra/spark/submit.sh` | Mode client : le driver tourne dans le conteneur du job, les executors le joignent par son IP |
| `infra/ingestion/Dockerfile` | Outil d'ingestion conteneurisé |
| `ingestion/download.py` | Téléchargement du dataset et contrôle d'empreinte SHA-256 |
| `ingestion/prepare.py` | Nettoyage du CSV, conversion UTC, `event_id` stable, séparation observations / labels, rejets |
| `ingestion/contract.py` | Contrat du message Kafka : champs autorisés, types, bornes AIS |
| `ingestion/producer.py` | Publication (clé = MMSI, `acks=all`, idempotence), débit réglable, rapport de livraison |
| `ingestion/verify.py` | Relecture de Kafka et comparaison au lot (manquants, doublons, contenu modifié) |
| `ingestion/slice_batch.py` | Extraction d'une période (test) à rejouer en direct |
| `.env.example` | Paramètres et mémoire, sans secret |
| `docs/donnees.md` | Provenance, licence, volume, sens d'un événement |

**Démontrer :** `docker compose up`, topics créés, publication et vérification, `hdfs dfsadmin -report` (2 DataNodes), `hdfs fsck` (réplication 2).

## Étudiant 2 : Voie froide et Analyse batch

**Rôle :** archiver le flux dans HDFS et produire les indicateurs historiques avec Spark SQL.

| Fichier | À savoir expliquer |
|---|---|
| `streaming/archive_to_hdfs.py` | Lecture Kafka en Structured Streaming, Parquet partitionné par `event_date`, checkpoints HDFS, mode `--once` (availableNow) |
| `common/schemas.py` | Schémas des messages, décodage JSON, conversion de `event_time` en timestamp |
| `common/spark.py` | Session Spark, **dédoublonnage sur `event_id`** (rejeux du producteur), lecture silver/bronze |
| `batch/indicators.py` | Couches bronze → silver, les 5 indicateurs SQL, les métriques de qualité |
| `common/db.py` (partie `replace_table`) | Écriture des indicateurs dans PostgreSQL |
| `infra/postgres/init.sql` (tables `kpi_*`) | Schéma des tables d'indicateurs |

**Démontrer :** fichiers Parquet dans HDFS (`/data/bronze`, `/data/silver`), relance du job sans doublon grâce au checkpoint, tables `kpi_*` remplies, onglet « Analyse historique » du tableau de bord.

## Étudiant 3 : Machine Learning

**Rôle :** préparer les données, comparer trois techniques, régler la meilleure, sauvegarder le modèle.

| Fichier | À savoir expliquer |
|---|---|
| `batch/train_model.py` | Découpage **temporel par escale** (apprentissage / validation / test), référence (médiane par port), comparaison LR / Random Forest / GBT, réglage sur la validation, évaluation finale unique sur le test, MAE par horizon, sauvegarde versionnée |
| `common/features.py` | Variables disponibles au moment de la prédiction, distance haversine au port, temps « physique » distance/vitesse, imputation et encodage **intégrés au PipelineModel**, règle anti-fuite |
| `common/config.py` (ports) | Coordonnées des ports, hypothèse sur CYZYY |
| `docs/results/metrics_latest.json` | Résultats chiffrés à commenter dans le rapport |
| `infra/postgres/init.sql` (tables `ml_*`, `actuals`) | Restitution des métriques et de la vérité terrain |

**Démontrer :** sortie du job d'entraînement, tableau comparatif, paramètres retenus, interprétation (erreur selon l'horizon, variables importantes, limites : 113 escales seulement), modèle présent dans HDFS (`/models/eta/<version>`).

## Étudiant 4 : Voie chaude et Restitution

**Rôle :** inférence temps réel, base de restitution, tableau de bord, preuve de reprise.

| Fichier | À savoir expliquer |
|---|---|
| `streaming/predict_stream.py` | Chargement du modèle au démarrage, `foreachBatch`, calcul de l'arrivée prédite et du délai, checkpoint |
| `common/db.py` (partie `insert_ignore_duplicates`) | Table de transit + `INSERT ... ON CONFLICT DO NOTHING` : la reprise n'écrit jamais deux fois |
| `infra/postgres/init.sql` (table `predictions`) | Clé primaire `event_id`, index |
| `dashboard/app.py` | Les trois onglets, l'actualisation automatique (`st.fragment(run_every=...)`), l'erreur réelle en direct (jointure avec `actuals`) |
| `dashboard/Dockerfile`, `dashboard/requirements.txt` | Image du tableau de bord, versions fixées |

**Démontrer :** flux actif jusqu'au tableau de bord, délai observé, arrêt brutal du job puis redémarrage sans perte ni doublon (requête de contrôle dans le README).

## Travail commun (les quatre)

- Points d'intégration réguliers (au moins un par semaine, idéalement deux vu le délai).
- `README.md` : chacun relit et teste les étapes de sa couche.
- Rapport (15-20 pages) : chacun rédige la section de sa couche ; le tableau des contributions est rempli ensemble.
- Vidéo de 10 minutes : cas métier (E1), architecture (E1), résultats ML (E3), flux actif et tableau de bord (E2 + E4).
