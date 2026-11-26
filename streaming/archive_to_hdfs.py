"""Voie froide : archiver en continu les topics Kafka dans HDFS (Parquet, partitionné par jour).

Deux requêtes Structured Streaming tournent dans la même application :
  maritime.observations.v1 -> /data/bronze/observations/event_date=AAAA-MM-JJ/
  maritime.labels.v1       -> /data/bronze/labels/event_date=AAAA-MM-JJ/
Les checkpoints HDFS mémorisent les offsets traités : après un arrêt, le job reprend où il s'était arrêté.
Les doublons de rejeu (même event_id) sont conservés ici et retirés dans la couche silver (batch).
"""
import argparse

from pyspark.sql import functions as F

from common import config
from common.schemas import LABEL_SCHEMA, OBSERVATION_SCHEMA, parse_kafka
from common.spark import session


def start_archive(spark, topic, schema, target, checkpoint, args):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_offsets)
           .option("failOnDataLoss", "false")
           .load())
    events = (parse_kafka(raw, schema)
              .withColumn("event_date", F.to_date("event_ts"))
              .withColumn("archived_at", F.current_timestamp()))
    writer = (events.writeStream.format("parquet")
              .option("path", target)
              .option("checkpointLocation", checkpoint)
              .partitionBy("event_date")
              .outputMode("append")
              .queryName(f"archive_{topic.replace('.', '_')}"))
    # --once : traite tout ce qui est disponible puis s'arrête (constitution rapide de l'historique).
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime=args.trigger)
    return writer.start()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trigger", default="20 seconds", help="Intervalle des micro-batchs")
    parser.add_argument("--max-offsets", type=int, default=100000, help="Messages max par micro-batch")
    parser.add_argument("--starting-offsets", default="earliest",
                        help="Utilisé seulement au premier démarrage (sans checkpoint)")
    parser.add_argument("--once", action="store_true",
                        help="Traiter ce qui est disponible puis s'arrêter (trigger availableNow)")
    args = parser.parse_args()

    spark = session("archive-kafka-to-hdfs")
    queries = [
        start_archive(spark, config.OBSERVATIONS_TOPIC, OBSERVATION_SCHEMA, config.BRONZE_OBSERVATIONS,
                      f"{config.CHECKPOINTS_DIR}/archive_observations", args),
        start_archive(spark, config.LABELS_TOPIC, LABEL_SCHEMA, config.BRONZE_LABELS,
                      f"{config.CHECKPOINTS_DIR}/archive_labels", args),
    ]
    print(f"Archivage démarré : {[q.name for q in queries]}", flush=True)
    for query in queries:
        query.awaitTermination()
        progress = query.lastProgress or {}
        print(f"{query.name} terminé : dernier batch {progress.get('batchId')}", flush=True)


if __name__ == "__main__":
    main()
