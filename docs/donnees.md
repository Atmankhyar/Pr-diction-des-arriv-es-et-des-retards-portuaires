# Données : AIS2ETA

## Provenance et conditions d'utilisation

| Élément | Valeur |
|---|---|
| Source | Dépôt [cut-dicl/ais2eta](https://github.com/cut-dicl/ais2eta), fichier `dataset generation python code/all_data_final.zip` |
| Auteurs | N. Evmides, S. Aslam, T. Ramez, M. Michaelides, H. Herodotou (Cyprus University of Technology) |
| Publication | *Enhancing Prediction Accuracy of Vessel Arrival Times Using Machine Learning*, JMSE 12(8), 1362, 2024 |
| Licence | Apache 2.0 (fichier `LICENSE` du dépôt source) : réutilisation et redistribution autorisées avec mention |
| Téléchargement | `docker compose run --rm ingestion ingestion.download` (empreinte SHA-256 vérifiée) |
| Fichier | `data/raw/all_data_final.csv`, 158 Mo, non versionné dans Git (limite GitHub de 100 Mo) |

## Volume

- 1 006 348 observations AIS (lignes hors en-tête)
- 99 navires (MMSI), 113 escales
- Période : juillet et août 2020
- 3 ports de destination : `CYLMS` Limassol (60 %), `CYLCA` Larnaca (26 %), `CYZYY` rattaché à Vasiliko (14 %)
- 7 catégories de navires : CONTAINER, GENERAL CARGO, TANKER, OTHER, BULK CARRIER, OFFSHORE SUPPORT, PASSENGER

## Sens d'un événement

Un événement est **une observation AIS d'un navire à un instant donné**, pendant son approche d'un port chypriote.

| Rôle | Champ publié dans Kafka | Colonne CSV |
|---|---|---|
| Identifiant | `event_id` : SHA-256 des champs observables (stable d'un rejeu à l'autre) | calculé |
| Clé Kafka | `vessel_id` (MMSI, 9 chiffres) | `mmsi` |
| Horodatage | `event_time` (ISO 8601 UTC) | `received_timestamp` |
| Position | `latitude`, `longitude` | `latitude`, `longitude` |
| Mouvement | `speed_over_ground_knots`, `course_over_ground_deg`, `rate_of_turn_deg_min`, `true_heading_deg`, `navigation_status` | `sog`, `cog`, `rot`, `true_heading`, `nav_status` |
| Navire | `draught_m`, `breadth_m`, `length_m`, `vessel_type`, `ship_category` | `draught`, `breadth`, `length`, `vesselType`, `ShipCategory` |
| Voyage | `destination` (code UN/LOCODE déclaré par l'équipage) | `destination` |
| Qualité | `quality_flags` : champs AIS absents ou hors bornes (ex. cap 511 = non disponible) | calculé |

## Cible (label)

- **`remaining_hours`** : temps restant avant l'arrivée réelle au port, **en heures** (`arrival_time - received_timestamp`).
- Publiée dans un topic séparé `maritime.labels.v1`, jamais dans les observations : le modèle ne peut pas la lire au moment de prédire.
- Contrôle de cohérence à la préparation : l'écart avec la colonne source `actual_eta_in_min` doit rester inférieur à une minute.
- Distribution : médiane 10,4 h ; 90 % des valeurs sous 23 h ; maximum 97,4 h.

## Colonnes volontairement exclues

| Colonne CSV | Raison |
|---|---|
| `arrival_time`, `actual_eta_in_min`, `arrival_time_exists` | Connues seulement après l'arrivée : ce serait une fuite de la cible |
| `eta_total`, `agent_eta_in_min` | ETA déclarée par l'agent maritime, non vérifiée par le contrat d'ingestion |
| `distance_to_cover` | Recalculée par le pipeline (`distance_to_port_km`, haversine) pour rester maîtrisée |
| `k_fold` | Découpage propre à l'étude d'origine ; nous faisons un découpage temporel |

## Hypothèses à valider

- **Fuseau horaire** : les dates sont traitées comme UTC. Juillet-août ne contient aucun changement d'heure, et la cible est cohérente avec les horodatages.
- **CYZYY** : rattaché au port de Vasiliko (34,727 N ; 33,327 E). Les coordonnées des ports sont dans `common/config.py`.
