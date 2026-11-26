"""Schémas des messages Kafka (contrat défini dans ingestion/contract.py) et lecture du flux."""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType, StructField,
                               StructType)

OBSERVATION_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("source_timezone", StringType()),
    StructField("source_event_time", StringType()),
    StructField("vessel_id", StringType()),
    StructField("voyage_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_over_ground_knots", DoubleType()),
    StructField("course_over_ground_deg", DoubleType()),
    StructField("rate_of_turn_deg_min", DoubleType()),
    StructField("navigation_status", IntegerType()),
    StructField("true_heading_deg", IntegerType()),
    StructField("destination", StringType()),
    StructField("draught_m", DoubleType()),
    StructField("breadth_m", DoubleType()),
    StructField("length_m", DoubleType()),
    StructField("vessel_type", StringType()),
    StructField("ship_category", StringType()),
    StructField("quality_flags", ArrayType(StringType())),
    StructField("ingestion_time", StringType()),
    StructField("ingestion_run_id", StringType()),
    StructField("ingestion_batch_id", StringType()),
])

LABEL_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("vessel_id", StringType()),
    StructField("event_time", StringType()),
    StructField("voyage_id", StringType()),
    StructField("voyage_id_method", StringType()),
    StructField("actual_arrival_time", StringType()),
    StructField("remaining_hours", DoubleType()),
    StructField("source_actual_eta_minutes", DoubleType()),
    StructField("label_difference_minutes", DoubleType()),
])


def iso_to_timestamp(column):
    """Convertir une date ISO 8601 UTC ('...Z', avec ou sans millisecondes) en timestamp Spark."""
    return F.to_timestamp(F.regexp_replace(column, "Z$", ""))


def parse_kafka(raw: DataFrame, schema: StructType) -> DataFrame:
    """Décoder la valeur JSON d'un message Kafka et conserver ses coordonnées (partition, offset)."""
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("e"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("e.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
    # Un message illisible donne un event_id nul : on l'écarte plutôt que de casser le flux.
    parsed = parsed.where(F.col("event_id").isNotNull())
    return parsed.withColumn("event_ts", iso_to_timestamp(F.col("event_time")))
