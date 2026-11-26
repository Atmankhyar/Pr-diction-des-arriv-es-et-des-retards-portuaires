"""Chemins HDFS, topics Kafka et connexion PostgreSQL, lus depuis l'environnement Docker."""
import os

HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:9000")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka:9092")
OBSERVATIONS_TOPIC = os.getenv("KAFKA_OBSERVATIONS_TOPIC", "maritime.observations.v1")
LABELS_TOPIC = os.getenv("KAFKA_LABELS_TOPIC", "maritime.labels.v1")

# Couche "bronze" : flux Kafka archivé tel quel (avec doublons éventuels de rejeu).
BRONZE_OBSERVATIONS = f"{HDFS_URL}/data/bronze/observations"
BRONZE_LABELS = f"{HDFS_URL}/data/bronze/labels"
# Couche "silver" : historique dédoublonné sur event_id, prêt pour l'analyse et le ML.
SILVER_OBSERVATIONS = f"{HDFS_URL}/data/silver/observations"
SILVER_LABELS = f"{HDFS_URL}/data/silver/labels"
# Modèles versionnés : /models/eta/<version> et /models/eta/current (version servie).
MODELS_DIR = f"{HDFS_URL}/models/eta"
CURRENT_MODEL = f"{MODELS_DIR}/current"
CHECKPOINTS_DIR = f"{HDFS_URL}/checkpoints"

POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "port_eta"),
    "user": os.getenv("POSTGRES_USER", "port_eta"),
    "password": os.getenv("POSTGRES_PASSWORD", "port_eta"),
}
JDBC_URL = f"jdbc:postgresql://{POSTGRES['host']}:{POSTGRES['port']}/{POSTGRES['dbname']}"
JDBC_PROPERTIES = {"user": POSTGRES["user"], "password": POSTGRES["password"],
                   "driver": "org.postgresql.Driver"}

# Ports chypriotes du dataset (codes UN/LOCODE du champ destination).
# Coordonnées approximatives des bassins portuaires ; CYZYY est rattaché à Vasiliko (hypothèse documentée).
PORTS = {
    "CYLMS": ("Limassol", 34.6531, 33.0139),
    "CYLCA": ("Larnaca", 34.9170, 33.6400),
    "CYZYY": ("Vasiliko", 34.7270, 33.3270),
}
