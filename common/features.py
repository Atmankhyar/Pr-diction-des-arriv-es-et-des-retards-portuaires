"""Préparation des variables, intégrée au PipelineModel pour que l'inférence refasse les mêmes calculs.

Règle anti-fuite : seules les informations connues au moment de l'observation sont utilisées
(position, vitesse, cap, statut, dimensions, catégorie, destination déclarée, heure).
Les champs liés à l'arrivée (actual_arrival_time, voyage_id historique) ne sont jamais des variables.
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import (Imputer, OneHotEncoder, SQLTransformer, StandardScaler,
                                StringIndexer, VectorAssembler)

from common.config import PORTS

LABEL = "remaining_hours"

# Champs du message Kafka pouvant être nuls (valeurs AIS "non disponible" signalées dans quality_flags).
RAW_NUMERIC = ["latitude", "longitude", "speed_over_ground_knots", "course_over_ground_deg",
               "navigation_status", "draught_m", "breadth_m", "length_m"]
ENGINEERED_NUMERIC = ["distance_to_port_km", "naive_eta_hours", "hour_of_day", "day_of_week",
                      "is_stationary"]
CATEGORICAL = ["destination", "ship_category"]


def _case(index):
    branches = " ".join(f"WHEN '{code}' THEN {values[index]}" for code, values in PORTS.items())
    return f"CASE destination {branches} END"


# 1) Coordonnées du port déclaré puis distance orthodromique (haversine, km).
PORT_DISTANCE_SQL = f"""
SELECT p.*,
       2 * 6371.0 * ASIN(SQRT(
           POW(SIN(RADIANS(p.port_lat - p.latitude) / 2), 2) +
           COS(RADIANS(p.latitude)) * COS(RADIANS(p.port_lat)) *
           POW(SIN(RADIANS(p.port_lon - p.longitude) / 2), 2))) AS distance_to_port_km
FROM (SELECT *, {_case(1)} AS port_lat, {_case(2)} AS port_lon FROM __THIS__) p
"""

# 2) Variables dérivées : temps "physique" distance / vitesse, calendrier, navire à l'arrêt.
#    1 nœud = 1,852 km/h ; sous 0,5 nœud la division n'a pas de sens -> valeur nulle puis imputée.
DERIVED_SQL = """
SELECT *,
       CASE WHEN speed_over_ground_knots >= 0.5
            THEN LEAST(distance_to_port_km / (speed_over_ground_knots * 1.852), 500.0) END AS naive_eta_hours,
       CAST(HOUR(event_ts) AS DOUBLE) AS hour_of_day,
       CAST(DAYOFWEEK(event_ts) AS DOUBLE) AS day_of_week,
       CASE WHEN speed_over_ground_knots < 0.5 THEN 1.0 ELSE 0.0 END AS is_stationary
FROM __THIS__
"""


def feature_stages(scale=False):
    """Étapes communes à tous les modèles : calcul, imputation (apprise sur le train), encodage."""
    numeric = RAW_NUMERIC + ENGINEERED_NUMERIC
    imputed = [f"{name}_imp" for name in numeric]
    stages = [
        SQLTransformer(statement=PORT_DISTANCE_SQL),
        SQLTransformer(statement=DERIVED_SQL),
        Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
    ]
    encoded = []
    for column in CATEGORICAL:
        stages.append(StringIndexer(inputCol=column, outputCol=f"{column}_idx", handleInvalid="keep"))
        encoded.append(f"{column}_vec")
    stages.append(OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL], outputCols=encoded,
                                handleInvalid="keep"))
    assembler_output = "raw_features" if scale else "features"
    stages.append(VectorAssembler(inputCols=imputed + encoded, outputCol=assembler_output,
                                  handleInvalid="keep"))
    if scale:  # utile pour la régression linéaire, sans effet sur les arbres
        stages.append(StandardScaler(inputCol="raw_features", outputCol="features",
                                     withMean=True, withStd=True))
    return stages


def feature_names(model_pipeline):
    """Noms lisibles des colonnes du vecteur features (pour l'importance des variables)."""
    names = [f"{name}" for name in RAW_NUMERIC + ENGINEERED_NUMERIC]
    for stage in model_pipeline.stages:
        if stage.__class__.__name__ == "StringIndexerModel":
            column = stage.getInputCol()
            # OneHotEncoder (dropLast=True, handleInvalid=keep) : une colonne par modalité connue.
            names += [f"{column}={label}" for label in stage.labels]
    return names


def build_pipeline(estimator, scale=False):
    return Pipeline(stages=feature_stages(scale) + [estimator])
