"""Voie chaude : prédire en temps réel le temps restant avant l'arrivée de chaque navire.

Au démarrage, le job charge le PipelineModel servi (/models/eta/current), qui contient aussi
toutes les préparations (distance au port, imputation, encodage). Pour chaque micro-batch Kafka :
  observations -> modèle -> PostgreSQL (table predictions, clé event_id).
Reprise : le checkpoint HDFS rejoue le dernier micro-batch non validé ; l'insertion
ON CONFLICT DO NOTHING garantit qu'aucune prédiction n'est écrite deux fois.
"""
import argparse

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from common import config, db
from common.schemas import OBSERVATION_SCHEMA, iso_to_timestamp, parse_kafka
from common.spark import session

OUTPUT_COLUMNS = ["event_id", "vessel_id", "event_time", "destination", "ship_category",
                  "latitude", "longitude", "speed_over_ground_knots", "distance_to_port_km",
                  "predicted_remaining_hours", "predicted_arrival_time", "model_version",
                  "ingestion_time", "prediction_time", "latency_ms"]


def load_served_model(spark):
    model = PipelineModel.load(config.CURRENT_MODEL)
    version = spark.read.text(f"{config.MODELS_DIR}/current_version").first()[0].strip()
    return model, version


def make_batch_writer(model, version):
    def write_batch(batch, batch_id):
        batch = batch.dropDuplicates(["event_id"])
        if batch.isEmpty():
            return
        scored = model.transform(batch)
        hours = F.greatest(F.col("prediction"), F.lit(0.0))
        result = (scored
                  .withColumn("predicted_remaining_hours", hours)
                  .withColumn("predicted_arrival_time",
                              (F.col("event_ts").cast("double") + hours * 3600).cast("timestamp"))
                  .withColumn("event_time", F.col("event_ts"))
                  .withColumn("ingestion_time", iso_to_timestamp(F.col("ingestion_time")))
                  .withColumn("prediction_time", F.current_timestamp())
                  .withColumn("latency_ms", (F.col("prediction_time").cast("double")
                                             - F.col("ingestion_time").cast("double")) * 1000)
                  .withColumn("model_version", F.lit(version))
                  .select(*OUTPUT_COLUMNS))
        rows = result.cache()
        count = rows.count()
        db.insert_ignore_duplicates(rows, "predictions", "event_id")
        rows.unpersist()
        print(f"micro-batch {batch_id} : {count} prédictions écrites (modèle {version})", flush=True)
    return write_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--starting-offsets", default="latest",
                        help="latest = seulement les nouveaux événements (premier démarrage)")
    parser.add_argument("--max-offsets", type=int, default=5000)
    args = parser.parse_args()

    spark = session("predict-eta-stream")
    model, version = load_served_model(spark)
    print(f"Modèle chargé : {config.CURRENT_MODEL} (version {version})", flush=True)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
           .option("subscribe", config.OBSERVATIONS_TOPIC)
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_offsets)
           .option("failOnDataLoss", "false")
           .load())
    observations = parse_kafka(raw, OBSERVATION_SCHEMA)

    query = (observations.writeStream
             .foreachBatch(make_batch_writer(model, version))
             .option("checkpointLocation", f"{config.CHECKPOINTS_DIR}/predict_eta")
             .trigger(processingTime=args.trigger)
             .queryName("predict_eta")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
