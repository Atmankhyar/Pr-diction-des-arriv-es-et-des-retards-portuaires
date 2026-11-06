# Prédiction des arrivées et des retards portuaires

Ce projet consiste à construire un pipeline Big Data pour collecter les observations de navires, conserver leur historique et prédire leur temps restant avant l’arrivée au port.

La première étape porte sur l’ingestion des données dans Kafka.

## Données utilisées

Le dataset provient du projet [AIS2ETA](https://github.com/cut-dicl/ais2eta). Il contient environ un million d’observations AIS concernant des navires faisant escale à Chypre.

Les observations comprennent notamment la position, la vitesse, la direction et les caractéristiques du navire.

Un événement représente une observation d’un navire à un instant donné. Les résultats historiques utilisés pour l’apprentissage sont séparés des observations destinées à la prédiction.

## Étape 1 — Ingestion

Le traitement mis en place suit ce parcours :

**CSV → préparation des données → producteur Python → Kafka → contrôle de réception**

### Préparation des données

Le script `ingestion/prepare.py` prépare le CSV avant publication :

- Vérification des colonnes et des valeurs obligatoires.
- Conversion des dates selon le fuseau indiqué.
- Suppression des doublons et tri chronologique du lot.
- Séparation des observations et des résultats historiques.
- Enregistrement des lignes rejetées avec leur motif.

Cette préparation produit quatre fichiers :

| Fichier | Contenu |
|---|---|
| `observations.jsonl` | Observations à publier dans Kafka |
| `labels.jsonl` | Résultats historiques pour l’apprentissage |
| `rejects.jsonl` | Enregistrements rejetés et motifs |
| `manifest.json` | Informations sur la source et bilan de préparation |

### Publication dans Kafka

Le script `ingestion/producer.py` publie les observations au format JSON dans le topic `maritime.observations.v1`.

Chaque observation possède un identifiant stable, `event_id`. L’identifiant du navire, `vessel_id`, sert de clé Kafka.

Le producteur permet de régler le débit d’envoi et enregistre les confirmations de livraison ainsi que les erreurs éventuelles.

Kafka fonctionne dans Docker avec un broker et trois partitions par topic. Un volume persistant conserve ses données.

### Contrôle de réception

Le script `ingestion/verify.py` relit les messages dans Kafka et les compare au lot préparé.

Il vérifie les identifiants, les clés et le contenu des messages afin de détecter les observations manquantes, supplémentaires, modifiées ou dupliquées pour la publication contrôlée.

Les rapports sont enregistrés dans `data/reports/`.

## Tests et validation

Les tests présents couvrent la préparation des données, le contrat des événements et le comportement du producteur. Des tests d’intégration permettent également de vérifier la publication et la réception avec un broker Kafka.

La présence de ces tests ne signifie pas qu’ils ont tous été exécutés avec succès. Les résultats obtenus et les vérifications restantes sont consignés dans [docs/validation.md](docs/validation.md).

Le schéma des événements et les règles de qualité sont décrits dans [docs/data-contract.md](docs/data-contract.md).

## Points restant à confirmer

- Le fuseau horaire des dates du dataset.
- Certaines unités et la définition précise de l’arrivée au port.
- Les conditions de redistribution du dataset.

Lors d’un rejeu, le producteur republie le lot depuis le début. Les identifiants des observations restent identiques ; la gestion des doublons devra être intégrée aux traitements suivants.

## Prochaines étapes

1. Lire les événements Kafka avec Spark Structured Streaming.
2. Archiver les observations dans HDFS au format Parquet.
3. Calculer les indicateurs historiques avec Spark SQL.
4. Préparer les données d’apprentissage et comparer trois modèles.
5. Intégrer les prédictions en streaming.
6. Enregistrer et afficher les résultats.