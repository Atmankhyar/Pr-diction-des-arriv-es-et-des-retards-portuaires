-- Base de restitution : prédictions temps réel, indicateurs batch et suivi du modèle.

-- Voie chaude : une ligne par observation prédite (event_id unique => reprise sans doublon).
CREATE TABLE IF NOT EXISTS predictions (
    event_id              TEXT PRIMARY KEY,
    vessel_id             TEXT NOT NULL,
    event_time            TIMESTAMPTZ NOT NULL,
    destination           TEXT,
    ship_category         TEXT,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    speed_over_ground_knots DOUBLE PRECISION,
    distance_to_port_km   DOUBLE PRECISION,
    predicted_remaining_hours DOUBLE PRECISION NOT NULL,
    predicted_arrival_time TIMESTAMPTZ NOT NULL,
    model_version         TEXT NOT NULL,
    ingestion_time        TIMESTAMPTZ,
    prediction_time       TIMESTAMPTZ NOT NULL,
    latency_ms            DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS predictions_vessel_time ON predictions (vessel_id, event_time DESC);
CREATE INDEX IF NOT EXISTS predictions_prediction_time ON predictions (prediction_time DESC);

-- Vérité terrain de la période de test, pour mesurer l'erreur des prédictions en direct.
CREATE TABLE IF NOT EXISTS actuals (
    event_id              TEXT PRIMARY KEY,
    actual_remaining_hours DOUBLE PRECISION NOT NULL,
    actual_arrival_time   TIMESTAMPTZ NOT NULL
);

-- Indicateurs batch (réécrits à chaque exécution de batch/indicators.py).
CREATE TABLE IF NOT EXISTS kpi_daily_traffic (
    day DATE PRIMARY KEY, observations BIGINT, vessels BIGINT,
    avg_speed_knots DOUBLE PRECISION, moving_share DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS kpi_hourly_traffic (
    hour_utc INT PRIMARY KEY, observations BIGINT, avg_speed_knots DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS kpi_category (
    ship_category TEXT PRIMARY KEY, vessels BIGINT, observations BIGINT, voyages BIGINT,
    avg_speed_knots DOUBLE PRECISION, avg_length_m DOUBLE PRECISION, avg_draught_m DOUBLE PRECISION,
    avg_approach_hours DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS kpi_destination (
    destination TEXT PRIMARY KEY, port_name TEXT, vessels BIGINT, voyages BIGINT,
    observations BIGINT, median_remaining_hours DOUBLE PRECISION, avg_approach_hours DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS kpi_daily_arrivals (
    day DATE, destination TEXT, arrivals BIGINT, PRIMARY KEY (day, destination)
);
CREATE TABLE IF NOT EXISTS kpi_data_quality (
    metric TEXT PRIMARY KEY, value DOUBLE PRECISION, description TEXT
);

-- Suivi ML : métriques de chaque modèle comparé, importance des variables, version servie.
CREATE TABLE IF NOT EXISTS ml_metrics (
    model_version TEXT, model_name TEXT, phase TEXT,
    mae_hours DOUBLE PRECISION, rmse_hours DOUBLE PRECISION, r2 DOUBLE PRECISION,
    params TEXT, train_rows BIGINT, test_rows BIGINT, trained_at TIMESTAMPTZ,
    PRIMARY KEY (model_version, model_name, phase)
);
CREATE TABLE IF NOT EXISTS ml_feature_importance (
    model_version TEXT, feature TEXT, importance DOUBLE PRECISION,
    PRIMARY KEY (model_version, feature)
);
