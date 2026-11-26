# Prédiction des arrivées et des retards portuaires

Pipeline Big Data de bout en bout qui collecte les observations AIS de navires en approche des ports de Chypre, conserve leur historique dans HDFS, entraîne un modèle Spark ML et **prédit en temps réel le temps restant avant l'arrivée au port** (régression, en heures).

```
CSV AIS2ETA → producteur Python → Kafka → Spark Structured Streaming ─┬─ voie froide : HDFS (Parquet) → Spark SQL + Spark ML → modèle versionné
                                                                       └─ voie chaude : modèle chargé au démarrage → prédictions
                                                     PostgreSQL (indicateurs + prédictions) → tableau de bord Streamlit
```

| Document | Contenu |
|---|---|
| [docs/donnees.md](docs/donnees.md) | Provenance, licence, volume, sens d'un événement, cible, colonnes exclues |
| [docs/architecture.md](docs/architecture.md) | Schéma des services et des flux, rôle des conteneurs, organisation HDFS, reprise |
| [docs/roles.md](docs/roles.md) | Répartition entre les 4 étudiants et fichiers que chacun doit maîtriser |
| `docs/results/metrics_latest.json` | Résultats ML produits par l'entraînement (créé au premier entraînement) |

## Cas métier

Un port planifie ses quais, ses pilotes et ses remorqueurs à partir de l'heure d'arrivée annoncée des navires, souvent imprécise. Le modèle estime, à chaque nouvelle position AIS, **le nombre d'heures restant avant l'arrivée réelle**.

- **Données :** [AIS2ETA](https://github.com/cut-dicl/ais2eta), licence Apache 2.0. 1 006 348 observations, 99 navires, 113 escales vers Limassol, Larnaca et Vasiliko, juillet-août 2020.
- **Cible :** `remaining_hours` = arrivée réelle − instant de l'observation (heures). Médiane 10,4 h.
- **Découpage :** temporel, par escale. Les escales les plus récentes forment le test.
- **Modèles comparés :** référence (médiane par port), régression linéaire, forêt aléatoire, gradient boosting (GBT). Réglage de la meilleure technique. Métriques : MAE et RMSE (heures), R².

## Topologie du cluster

| Couche | Services |
|---|---|
| Ingestion | `kafka` (KRaft, 3 partitions), `kafka-init`, outil `ingestion` |
| Stockage | `namenode` + `datanode1` + `datanode2`, **réplication HDFS = 2**, un volume persistant par DataNode |
| Calcul | `spark-master` + `spark-worker-1` + `spark-worker-2` (Spark 3.5.3, PySpark) |
| Jobs Spark | `stream-archive` (voie froide), `stream-predict` (voie chaude), `batch` (indicateurs, entraînement) |
| Restitution | `postgres` (PostgreSQL 16), `dashboard` (Streamlit) |

Interfaces : HDFS http://localhost:9870 · Spark http://localhost:8080 · Tableau de bord http://localhost:8501 · PostgreSQL `localhost:55432`.

## Prérequis

- Docker Desktop avec Docker Compose v2.
- **Au moins 4 Go de mémoire alloués à Docker** (valeurs par défaut réglées pour 4 Go ; augmenter `SPARK_*_MEMORY` dans `.env` si possible).
- **Environ 8 Go d'espace disque libre** (images ≈ 4 Go, données Kafka + HDFS ≈ 2 Go).
- Connexion Internet au premier lancement (images, connecteurs Maven, dataset).

## Lancement pas à pas

Toutes les commandes se lancent depuis la racine du dépôt (PowerShell, bash ou zsh).

### 1. Démarrer le cluster

```bash
cp .env.example .env          # facultatif : valeurs par défaut identiques
docker compose up -d --build
docker compose ps
```

Vérifier HDFS (2 DataNodes vivants) :

```bash
docker compose run --rm hdfs dfsadmin -report
```

### 2. Récupérer et préparer les données

```bash
docker compose run --rm ingestion ingestion.download
docker compose run --rm ingestion ingestion.prepare --output data/prepared/full --source-timezone UTC --limit 0
```

`prepare` produit `observations.jsonl`, `labels.jsonl`, `rejects.jsonl` et `manifest.json` dans `data/prepared/full`. Pour un essai rapide, utiliser `--limit 50000` et un autre dossier.

### 3. Constituer l'historique (voie froide)

Démarrer l'archivage Kafka → HDFS, puis publier tout le lot avec ses labels :

```bash
docker compose --profile streaming up -d stream-archive
docker compose run --rm ingestion ingestion.producer --batch data/prepared/full --publish --include-labels --rate 0
docker compose run --rm ingestion ingestion.verify --summary data/reports/<ingestion_run_id>.summary.json
```

Le nom du fichier de bilan est affiché par le producteur (`"summary": ...`). Suivre l'archivage avec `docker compose logs -f stream-archive`, puis contrôler HDFS :

```bash
docker compose run --rm hdfs dfs -du -h /data/bronze
docker compose run --rm hdfs fsck /data/bronze      # le résumé doit afficher "Average block replication: 2.0"
```

Quand le volume ne bouge plus, arrêter l'archivage pour libérer de la mémoire sur un petit poste :

```bash
docker compose stop stream-archive
```

### 4. Indicateurs historiques (Spark SQL)

```bash
docker compose run --rm batch /opt/app/batch/indicators.py
```

Le job dédoublonne le flux (couche silver dans HDFS) et remplit les tables `kpi_*` dans PostgreSQL.

### 5. Entraînement, comparaison et réglage (Spark ML)

```bash
docker compose run --rm batch /opt/app/batch/train_model.py
```

Sur un poste avec 4 Go pour Docker, ajouter `--sample 0.3` pour entraîner sur 30 % des observations. Le job affiche les métriques de chaque modèle et sauvegarde :

- le modèle dans HDFS : `/models/eta/<version>` et `/models/eta/current` (version servie) ;
- les métriques dans PostgreSQL (`ml_metrics`, `ml_feature_importance`) et dans `docs/results/metrics_latest.json`.

### 6. Prédiction en temps réel (voie chaude)

```bash
docker compose --profile streaming up -d stream-predict
docker compose logs -f stream-predict          # attendre "Modèle chargé"
```

Rejouer comme un flux en direct les observations de la **période de test**, jamais vues par le modèle. Prendre la date `test_cutoff_arrival` de `docs/results/metrics_latest.json` :

```bash
docker compose run --rm ingestion ingestion.slice_batch --batch data/prepared/full --output data/prepared/live --from-time 2020-08-25T00:00:00Z
docker compose run --rm ingestion ingestion.producer --batch data/prepared/live --publish --rate 20
```

### 7. Tableau de bord

Ouvrir http://localhost:8501. Trois onglets, actualisés automatiquement toutes les 5 s :

- **Temps réel :** dernière prédiction par navire, carte, débit, délai observé, erreur réelle.
- **Analyse historique :** indicateurs batch.
- **Modèle :** comparaison des techniques, réglage, variables importantes.

### 8. Vérifier la reprise

Pendant que le producteur publie, arrêter brutalement la voie chaude puis la relancer :

```bash
docker compose kill stream-predict
docker compose --profile streaming up -d stream-predict
```

Contrôler qu'il n'y a ni doublon ni trou :

```bash
docker compose exec postgres psql -U port_eta -d port_eta -c "SELECT COUNT(*) AS lignes, COUNT(DISTINCT event_id) AS evenements, MAX(prediction_time) FROM predictions;"
```

`lignes = evenements` (clé `event_id` + insertion `ON CONFLICT DO NOTHING`), et le total rattrape le nombre de messages publiés grâce au checkpoint HDFS.

### Arrêt et remise à zéro

```bash
docker compose --profile streaming --profile batch down          # garde les volumes
docker compose --profile streaming --profile batch down -v       # efface Kafka, HDFS et PostgreSQL
```

## Organisation du dépôt

```
ingestion/   Préparation du CSV, contrat des messages, producteur Kafka, contrôle, extraction d'une période
streaming/   archive_to_hdfs.py (voie froide), predict_stream.py (voie chaude)
batch/       indicators.py (Spark SQL), train_model.py (Spark ML)
common/      Configuration, schémas Kafka, préparation ML (PipelineModel), accès PostgreSQL
dashboard/   Application Streamlit
infra/       Dockerfiles, configuration Spark/Hadoop, schéma PostgreSQL
docs/        Données, architecture, rôles, résultats ML
compose.yaml         Cluster complet
compose.kafka.yaml   Kafka seul (livraison ingestion de l'étape 1)
```

## Choix et limites

- **Anti-fuite :** les labels voyagent dans un topic séparé. Les variables du modèle sont uniquement celles connues à l'instant de l'observation. Les préparations (distance au port, imputation, encodage) sont apprises sur l'entraînement et intégrées au `PipelineModel`.
- **Doublons de rejeu :** l'archive bronze garde tout. La couche silver ne garde qu'une ligne par `event_id`. Les prédictions sont insérées sans doublon.
- **Limites :** 113 escales seulement, sur deux mois d'été et trois ports. Le modèle reste moins précis pour les longs horizons (voir la MAE par horizon). Un seul broker Kafka (réplication 1) dans cette démonstration.
- **Hypothèses à confirmer :** horodatages en UTC ; `CYZYY` rattaché à Vasiliko (voir `docs/donnees.md`).
