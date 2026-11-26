"""Spark ML : entraîner, comparer, régler et sauvegarder le modèle de prédiction du temps restant.

Tâche : régression. Cible = remaining_hours, temps restant avant l'arrivée au port, en heures.

Protocole (aucune donnée du futur dans l'apprentissage) :
  1. Découpage temporel PAR ESCALE : les escales arrivées avant la date de coupure forment
     l'entraînement, les suivantes le test. Une même escale n'est jamais des deux côtés.
     L'entraînement est lui-même coupé dans le temps en "apprentissage" et "validation".
  2. Référence simple : médiane du temps restant par port, calculée sur l'entraînement.
  3. Trois techniques comparées : régression linéaire, forêt aléatoire, gradient boosting.
  4. La meilleure sur la validation est réglée (grille de paramètres évaluée sur la validation),
     réentraînée sur tout l'entraînement puis évaluée une seule fois sur le test.
  5. Le PipelineModel complet (préparations + modèle) est sauvegardé dans HDFS et versionné.
"""
import argparse
from datetime import datetime, timezone
import itertools
import json
from pathlib import Path

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import functions as F

from common import config, db
from common.features import LABEL, build_pipeline, feature_names
from common.schemas import iso_to_timestamp
from common.spark import read_history, session

SEED = 42


def candidates():
    """Les trois techniques comparées, avec leurs paramètres de départ et leur grille de réglage."""
    return {
        "linear_regression": dict(
            make=lambda **p: LinearRegression(featuresCol="features", labelCol=LABEL, maxIter=50, **p),
            scale=True, base={"regParam": 0.0, "elasticNetParam": 0.0},
            grid={"regParam": [0.01, 0.1], "elasticNetParam": [0.0, 0.5]}),
        "random_forest": dict(
            make=lambda **p: RandomForestRegressor(featuresCol="features", labelCol=LABEL, seed=SEED,
                                                   subsamplingRate=0.7, **p),
            scale=False, base={"numTrees": 30, "maxDepth": 8},
            grid={"numTrees": [30, 60], "maxDepth": [8, 12]}),
        "gbt": dict(
            make=lambda **p: GBTRegressor(featuresCol="features", labelCol=LABEL, seed=SEED,
                                          subsamplingRate=0.8, **p),
            scale=False, base={"maxIter": 40, "maxDepth": 5, "stepSize": 0.1},
            grid={"maxIter": [60], "maxDepth": [4, 6], "stepSize": [0.05, 0.15]}),
    }


def evaluate(predictions):
    metrics = {}
    for name in ("mae", "rmse", "r2"):
        metrics[name] = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction",
                                            metricName=name).evaluate(predictions)
    return metrics


def temporal_split(data, column, share):
    """Couper les escales selon leur date d'arrivée : les `share` plus récentes vont à droite."""
    voyages = data.groupBy("voyage_id").agg(F.max(column).alias("arrival"))
    cutoff = voyages.selectExpr(f"percentile_approx(arrival, {1 - share}) AS c").first().c
    tagged = data.join(voyages, "voyage_id")
    return tagged.where(F.col("arrival") <= cutoff).drop("arrival"), \
        tagged.where(F.col("arrival") > cutoff).drop("arrival"), cutoff


def baseline(train, test):
    """Référence : médiane du temps restant par port (médiane globale si port inconnu)."""
    medians = train.groupBy("destination").agg(F.percentile_approx(LABEL, 0.5).alias("median"))
    overall = train.select(F.percentile_approx(LABEL, 0.5)).first()[0]
    scored = test.join(medians, "destination", "left") \
        .withColumn("prediction", F.coalesce("median", F.lit(float(overall))))
    return evaluate(scored)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=float, default=1.0,
                        help="Fraction des observations utilisées (réduire sur une petite machine)")
    parser.add_argument("--test-share", type=float, default=0.2, help="Part des escales en test")
    parser.add_argument("--validation-share", type=float, default=0.2,
                        help="Part des escales d'entraînement gardées pour la validation")
    parser.add_argument("--skip-tuning", action="store_true")
    args = parser.parse_args()

    spark = session("train-eta-model")
    observations, labels = read_history(spark)
    data = (observations.drop("voyage_id")
            .join(labels.select("event_id", LABEL, "voyage_id",
                                iso_to_timestamp(F.col("actual_arrival_time")).alias("arrival_ts")),
                  "event_id")
            .where(F.col(LABEL).isNotNull() & (F.col(LABEL) >= 0)))
    if args.sample < 1.0:
        data = data.sample(fraction=args.sample, seed=SEED)
    data = data.cache()

    train, test, test_cutoff = temporal_split(data, "arrival_ts", args.test_share)
    fit_part, validation, val_cutoff = temporal_split(train, "arrival_ts", args.validation_share)
    for frame in (train, test, fit_part, validation):
        frame.cache()
    sizes = {name: frame.count() for name, frame in
             (("train", train), ("test", test), ("fit", fit_part), ("validation", validation))}
    print(f"Coupures : validation après {val_cutoff}, test après {test_cutoff}", flush=True)
    print(f"Tailles : {sizes}", flush=True)

    version = datetime.now(timezone.utc).strftime("v%Y%m%dT%H%M%SZ")
    trained_at = datetime.now(timezone.utc)
    rows = []

    def record(model_name, phase, metrics, params, test_rows):
        rows.append((version, model_name, phase, metrics["mae"], metrics["rmse"], metrics["r2"],
                     json.dumps(params), sizes["train"], test_rows, trained_at))
        print(f"  {model_name:<20} {phase:<12} MAE={metrics['mae']:.2f} h  RMSE={metrics['rmse']:.2f} h  "
              f"R2={metrics['r2']:.3f}  {params}", flush=True)

    print("Référence simple (médiane par port) :", flush=True)
    record("baseline_median_port", "validation", baseline(fit_part, validation), {}, sizes["validation"])
    record("baseline_median_port", "test", baseline(train, test), {}, sizes["test"])

    print("Comparaison des trois techniques (apprentissage -> validation) :", flush=True)
    models = candidates()
    scores = {}
    for name, spec in models.items():
        fitted = build_pipeline(spec["make"](**spec["base"]), spec["scale"]).fit(fit_part)
        scores[name] = evaluate(fitted.transform(validation))
        record(name, "validation", scores[name], spec["base"], sizes["validation"])
    best_name = min(scores, key=lambda n: scores[n]["rmse"])
    best = models[best_name]
    best_params = dict(best["base"])
    print(f"Meilleure technique sur la validation : {best_name}", flush=True)

    if not args.skip_tuning:
        print(f"Réglage de {best_name} (grille évaluée sur la validation) :", flush=True)
        best_rmse = scores[best_name]["rmse"]
        keys = list(best["grid"])
        for values in itertools.product(*(best["grid"][k] for k in keys)):
            params = dict(best["base"], **dict(zip(keys, values)))
            fitted = build_pipeline(best["make"](**params), best["scale"]).fit(fit_part)
            metrics = evaluate(fitted.transform(validation))
            record(f"{best_name}_tuning", "validation", metrics, params, sizes["validation"])
            if metrics["rmse"] < best_rmse:
                best_rmse, best_params = metrics["rmse"], params
        print(f"Paramètres retenus : {best_params}", flush=True)

    # Modèle final : meilleurs paramètres, réentraîné sur tout l'entraînement, évalué une fois sur le test.
    final = build_pipeline(best["make"](**best_params), best["scale"]).fit(train)
    test_predictions = final.transform(test).cache()
    final_metrics = evaluate(test_predictions)
    record(f"{best_name}_final", "test", final_metrics, best_params, sizes["test"])

    # Erreur par horizon : une prédiction à 30 h est naturellement moins précise qu'à 2 h.
    horizon = (test_predictions
               .withColumn("horizon", F.when(F.col(LABEL) < 6, "0-6 h").when(F.col(LABEL) < 12, "6-12 h")
                           .when(F.col(LABEL) < 24, "12-24 h").otherwise("24 h et +"))
               .groupBy("horizon")
               .agg(F.count("*").alias("rows"),
                    F.round(F.avg(F.abs(F.col("prediction") - F.col(LABEL))), 2).alias("mae_hours"))
               .orderBy("horizon").collect())
    print("MAE du modèle final par horizon :", flush=True)
    for row in horizon:
        print(f"  {row.horizon:<10} {row.rows:>8} lignes  MAE={row.mae_hours} h", flush=True)

    # Sauvegarde versionnée + version servie au job de streaming.
    path = f"{config.MODELS_DIR}/{version}"
    final.write().overwrite().save(path)
    final.write().overwrite().save(config.CURRENT_MODEL)
    spark.createDataFrame([(version,)], "value STRING").coalesce(1) \
        .write.mode("overwrite").text(f"{config.MODELS_DIR}/current_version")
    print(f"Modèle sauvegardé : {path} (copie servie : {config.CURRENT_MODEL})", flush=True)

    # Importance des variables (arbres) ou valeur absolue des coefficients (linéaire).
    estimator = final.stages[-1]
    weights = (estimator.featureImportances.toArray().tolist() if hasattr(estimator, "featureImportances")
               else [abs(c) for c in estimator.coefficients.toArray().tolist()])
    names = feature_names(final)
    if len(names) != len(weights):
        names = [f"f{i}" for i in range(len(weights))]
    importance = sorted(zip(names, weights), key=lambda item: -item[1])

    # Restitution dans PostgreSQL : métriques, importances, vérité terrain du test.
    db.execute("DELETE FROM ml_metrics WHERE model_version = %s", (version,))
    spark.createDataFrame(rows, "model_version STRING, model_name STRING, phase STRING, mae_hours DOUBLE, "
                                "rmse_hours DOUBLE, r2 DOUBLE, params STRING, train_rows LONG, "
                                "test_rows LONG, trained_at TIMESTAMP") \
        .write.mode("append").jdbc(config.JDBC_URL, "ml_metrics", properties=config.JDBC_PROPERTIES)
    spark.createDataFrame([(version, n, float(w)) for n, w in importance],
                          "model_version STRING, feature STRING, importance DOUBLE") \
        .write.mode("append").jdbc(config.JDBC_URL, "ml_feature_importance",
                                   properties=config.JDBC_PROPERTIES)
    actuals = test.select("event_id", F.col(LABEL).alias("actual_remaining_hours"),
                          F.col("arrival_ts").alias("actual_arrival_time"))
    db.insert_ignore_duplicates(actuals, "actuals", "event_id")

    # Trace dans le dépôt (docs/results) pour le rapport.
    summary = {
        "version": version, "task": "regression", "target": "remaining_hours (heures)",
        "test_cutoff_arrival": str(test_cutoff), "validation_cutoff_arrival": str(val_cutoff),
        "sizes": sizes, "best_model": best_name, "best_params": best_params,
        "metrics": [dict(zip(["model_version", "model_name", "phase", "mae_hours", "rmse_hours", "r2",
                              "params"], r[:7])) for r in rows],
        "test_mae_by_horizon": [r.asDict() for r in horizon],
        "feature_importance_top15": importance[:15],
    }
    try:
        out = Path("/opt/app/docs/results")
        out.mkdir(parents=True, exist_ok=True)
        text = json.dumps(summary, ensure_ascii=False, indent=2, default=str)
        (out / f"metrics_{version}.json").write_text(text, encoding="utf-8")
        (out / "metrics_latest.json").write_text(text, encoding="utf-8")
    except OSError as exc:
        print(f"Résumé non écrit dans docs/results : {exc}", flush=True)
    print(json.dumps({"version": version, "best_model": best_name, "test": final_metrics}), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
