"""Analyse batch : construire l'historique dédoublonné (silver) et calculer les indicateurs avec Spark SQL.

Entrée : /data/bronze/{observations,labels} (archivés par streaming/archive_to_hdfs.py)
Sorties : /data/silver/{observations,labels} dans HDFS, tables kpi_* dans PostgreSQL.
"""
import argparse

from pyspark.sql import functions as F

from common import config, db
from common.spark import deduplicate, session

INDICATORS = {
    # Trafic par jour : volume d'observations, navires actifs, vitesse moyenne, part des navires en mouvement.
    "kpi_daily_traffic": """
        SELECT CAST(event_ts AS DATE) AS day,
               COUNT(*) AS observations,
               COUNT(DISTINCT vessel_id) AS vessels,
               ROUND(AVG(speed_over_ground_knots), 2) AS avg_speed_knots,
               ROUND(AVG(CASE WHEN speed_over_ground_knots >= 0.5 THEN 1.0 ELSE 0.0 END), 3) AS moving_share
        FROM observations GROUP BY CAST(event_ts AS DATE) ORDER BY day""",
    # Profil horaire (UTC) des observations reçues.
    "kpi_hourly_traffic": """
        SELECT HOUR(event_ts) AS hour_utc, COUNT(*) AS observations,
               ROUND(AVG(speed_over_ground_knots), 2) AS avg_speed_knots
        FROM observations GROUP BY HOUR(event_ts) ORDER BY hour_utc""",
    # Par catégorie de navire : taille de flotte, vitesse, dimensions et durée moyenne d'approche.
    "kpi_category": """
        SELECT o.ship_category,
               COUNT(DISTINCT o.vessel_id) AS vessels,
               COUNT(*) AS observations,
               COUNT(DISTINCT l.voyage_id) AS voyages,
               ROUND(AVG(o.speed_over_ground_knots), 2) AS avg_speed_knots,
               ROUND(AVG(o.length_m), 1) AS avg_length_m,
               ROUND(AVG(o.draught_m), 2) AS avg_draught_m,
               ROUND(AVG(v.approach_hours), 1) AS avg_approach_hours
        FROM observations o
        LEFT JOIN labels l ON o.event_id = l.event_id
        LEFT JOIN voyages v ON l.voyage_id = v.voyage_id
        WHERE o.ship_category IS NOT NULL
        GROUP BY o.ship_category ORDER BY observations DESC""",
    # Par port de destination : fréquentation et temps restant typique.
    "kpi_destination": """
        SELECT o.destination, p.port_name,
               COUNT(DISTINCT o.vessel_id) AS vessels,
               COUNT(DISTINCT l.voyage_id) AS voyages,
               COUNT(*) AS observations,
               ROUND(PERCENTILE_APPROX(l.remaining_hours, 0.5), 1) AS median_remaining_hours,
               ROUND(AVG(v.approach_hours), 1) AS avg_approach_hours
        FROM observations o
        LEFT JOIN labels l ON o.event_id = l.event_id
        LEFT JOIN voyages v ON l.voyage_id = v.voyage_id
        LEFT JOIN ports p ON o.destination = p.destination
        WHERE o.destination IS NOT NULL
        GROUP BY o.destination, p.port_name ORDER BY observations DESC""",
    # Arrivées réelles par jour et par port (une escale = un voyage_id historique).
    "kpi_daily_arrivals": """
        SELECT CAST(arrival_ts AS DATE) AS day, destination, COUNT(*) AS arrivals
        FROM voyages GROUP BY CAST(arrival_ts AS DATE), destination ORDER BY day, destination""",
}


def build_silver(spark):
    """Dédoublonner le bronze sur event_id et réécrire la couche silver. Retourne les volumes."""
    bronze_obs = spark.read.parquet(config.BRONZE_OBSERVATIONS)
    bronze_labels = spark.read.parquet(config.BRONZE_LABELS)
    observations = deduplicate(bronze_obs)
    labels = deduplicate(bronze_labels)
    observations.write.mode("overwrite").partitionBy("event_date").parquet(config.SILVER_OBSERVATIONS)
    labels.write.mode("overwrite").partitionBy("event_date").parquet(config.SILVER_LABELS)
    return bronze_obs.count(), bronze_labels.count()


def quality_metrics(spark, bronze_obs_rows, bronze_label_rows):
    """Indicateurs de qualité : doublons de rejeu, couverture des labels, valeurs AIS manquantes."""
    totals = spark.sql("SELECT (SELECT COUNT(*) FROM observations) AS obs, "
                       "(SELECT COUNT(*) FROM labels) AS labels, "
                       "(SELECT COUNT(DISTINCT vessel_id) FROM observations) AS vessels, "
                       "(SELECT COUNT(*) FROM voyages) AS voyages").first()
    rows = [
        ("bronze_observations", float(bronze_obs_rows), "Messages archivés depuis Kafka (avec rejeux)"),
        ("silver_observations", float(totals.obs), "Observations uniques après dédoublonnage"),
        ("duplicates_removed", float(bronze_obs_rows - totals.obs), "Doublons de rejeu retirés"),
        ("bronze_labels", float(bronze_label_rows), "Labels archivés depuis Kafka"),
        ("silver_labels", float(totals.labels), "Labels uniques"),
        ("label_coverage_pct", round(100.0 * totals.labels / max(totals.obs, 1), 2),
         "Part des observations ayant une arrivée historique connue"),
        ("vessels", float(totals.vessels), "Navires distincts (MMSI)"),
        ("voyages", float(totals.voyages), "Escales distinctes"),
    ]
    flags = spark.sql("""
        SELECT flag, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM observations), 2) AS pct
        FROM (SELECT EXPLODE(quality_flags) AS flag FROM observations) GROUP BY flag""").collect()
    rows += [(f"missing_pct:{r.flag.split(':')[0]}", float(r.pct),
              "Part des observations où ce champ AIS est absent ou invalide") for r in flags]
    return spark.createDataFrame(rows, "metric STRING, value DOUBLE, description STRING")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-silver", action="store_true",
                        help="Réutiliser la couche silver existante au lieu de la reconstruire")
    args = parser.parse_args()
    spark = session("batch-indicators")

    bronze_obs_rows = bronze_label_rows = None
    if not args.skip_silver:
        bronze_obs_rows, bronze_label_rows = build_silver(spark)
        print(f"Silver reconstruit depuis {bronze_obs_rows} observations bronze", flush=True)

    spark.read.parquet(config.SILVER_OBSERVATIONS).createOrReplaceTempView("observations")
    spark.read.parquet(config.SILVER_LABELS).withColumn(
        "arrival_ts", F.to_timestamp(F.regexp_replace("actual_arrival_time", "Z$", ""))
    ).createOrReplaceTempView("labels")
    spark.createDataFrame([(code, name) for code, (name, _, _) in config.PORTS.items()],
                          "destination STRING, port_name STRING").createOrReplaceTempView("ports")
    # Une ligne par escale : arrivée réelle, port, durée d'approche observée (premier point -> arrivée).
    spark.sql("""
        SELECT l.voyage_id, FIRST(o.destination, true) AS destination, MAX(l.arrival_ts) AS arrival_ts,
               MAX(l.remaining_hours) AS approach_hours, COUNT(*) AS observations
        FROM labels l JOIN observations o ON o.event_id = l.event_id
        GROUP BY l.voyage_id""").cache().createOrReplaceTempView("voyages")

    for table, sql in INDICATORS.items():
        result = spark.sql(sql)
        db.replace_table(result, table)
        print(f"{table} : {result.count()} lignes", flush=True)

    if bronze_obs_rows is None:
        bronze_obs_rows = spark.read.parquet(config.BRONZE_OBSERVATIONS).count()
        bronze_label_rows = spark.read.parquet(config.BRONZE_LABELS).count()
    quality = quality_metrics(spark, bronze_obs_rows, bronze_label_rows)
    db.replace_table(quality, "kpi_data_quality")
    for row in quality.collect():
        print(f"  {row.metric:<38} {row.value}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
