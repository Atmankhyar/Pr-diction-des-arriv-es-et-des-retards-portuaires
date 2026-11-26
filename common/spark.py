"""Création de la session Spark et lecture de l'historique dédoublonné."""
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from common import config


def session(app_name: str) -> SparkSession:
    """Session connectée au cluster (master et HDFS définis dans spark-defaults.conf)."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def deduplicate(df: DataFrame) -> DataFrame:
    """Garder une seule ligne par event_id : la première reçue (offset Kafka le plus petit).

    Un rejeu du producteur republie les mêmes event_id ; la clé stable rend l'opération sûre.
    """
    order = Window.partitionBy("event_id").orderBy(F.col("kafka_timestamp"), F.col("kafka_offset"))
    return df.withColumn("_rank", F.row_number().over(order)).where("_rank = 1").drop("_rank")


def read_history(spark: SparkSession):
    """Lire observations et labels : silver s'il existe, sinon bronze dédoublonné à la volée."""
    try:
        observations = spark.read.parquet(config.SILVER_OBSERVATIONS)
        labels = spark.read.parquet(config.SILVER_LABELS)
    except Exception:  # silver absent : batch/indicators.py n'a pas encore tourné
        observations = deduplicate(spark.read.parquet(config.BRONZE_OBSERVATIONS))
        labels = deduplicate(spark.read.parquet(config.BRONZE_LABELS))
    return observations, labels
