"""Tableau de bord : prédictions en direct, indicateurs batch et résultats du modèle (PostgreSQL)."""
import os

import pandas as pd
import sqlalchemy
import streamlit as st

REFRESH = int(os.getenv("REFRESH_SECONDS", "5"))
ENGINE_URL = "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
    u=os.getenv("POSTGRES_USER", "port_eta"), p=os.getenv("POSTGRES_PASSWORD", "port_eta"),
    h=os.getenv("POSTGRES_HOST", "postgres"), port=os.getenv("POSTGRES_PORT", "5432"),
    d=os.getenv("POSTGRES_DB", "port_eta"))

st.set_page_config(page_title="ETA portuaire", page_icon="⚓", layout="wide")


@st.cache_resource
def engine():
    return sqlalchemy.create_engine(ENGINE_URL, pool_pre_ping=True)


def query(sql):
    try:
        with engine().connect() as connection:
            return pd.read_sql(sqlalchemy.text(sql), connection)
    except Exception as exc:  # base pas encore prête ou table vide : on affiche un message clair
        st.warning(f"Données indisponibles : {exc.__class__.__name__}")
        return pd.DataFrame()


st.title("Prédiction des arrivées portuaires")
st.caption("Navires en approche des ports de Chypre (dataset AIS2ETA). Temps restant prédit par Spark ML "
           f"en temps réel. Actualisation automatique toutes les {REFRESH} s.")

live, batch_tab, model_tab = st.tabs(["Temps réel", "Analyse historique", "Modèle"])


@st.fragment(run_every=REFRESH)
def live_view():
    summary = query("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE prediction_time > now() - interval '1 minute') AS last_minute,
               COUNT(DISTINCT vessel_id) AS vessels,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms)
                   FILTER (WHERE prediction_time > now() - interval '5 minutes') AS median_latency_ms,
               MAX(model_version) AS model_version
        FROM predictions""")
    error = query("""
        SELECT AVG(ABS(p.predicted_remaining_hours - a.actual_remaining_hours)) AS mae, COUNT(*) AS n
        FROM predictions p JOIN actuals a USING (event_id)""")
    if summary.empty or not summary.total[0]:
        st.info("Aucune prédiction pour l'instant. Lancez le job stream-predict puis rejouez un lot "
                "avec le producteur (voir README).")
        return
    s = summary.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Prédictions", f"{int(s.total):,}".replace(",", " "))
    c2.metric("Dernière minute", int(s.last_minute))
    c3.metric("Navires suivis", int(s.vessels))
    latency = s.median_latency_ms
    c4.metric("Délai médian (5 min)", "-" if pd.isna(latency) else f"{latency / 1000:.1f} s")
    if not error.empty and error.n[0]:
        c5.metric("Erreur moyenne réelle", f"{error.mae[0]:.1f} h", help=f"Sur {int(error.n[0])} prédictions "
                  "dont l'arrivée réelle est connue (période de test)")
    st.caption(f"Modèle servi : {s.model_version}")

    latest = query("""
        SELECT DISTINCT ON (p.vessel_id)
               p.vessel_id AS navire, p.ship_category AS categorie, p.destination,
               p.latitude, p.longitude, ROUND(p.speed_over_ground_knots::numeric, 1) AS vitesse_noeuds,
               ROUND(p.distance_to_port_km::numeric, 1) AS distance_km,
               p.event_time AS observe_a,
               ROUND(p.predicted_remaining_hours::numeric, 1) AS reste_predit_h,
               p.predicted_arrival_time AS arrivee_predite,
               ROUND(a.actual_remaining_hours::numeric, 1) AS reste_reel_h
        FROM predictions p LEFT JOIN actuals a USING (event_id)
        ORDER BY p.vessel_id, p.event_time DESC""")
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Dernière prédiction par navire")
        st.dataframe(latest.drop(columns=["latitude", "longitude"]).sort_values("reste_predit_h"),
                     hide_index=True, use_container_width=True)
    with right:
        st.subheader("Positions")
        st.map(latest.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]])

    flow = query("""
        SELECT date_trunc('minute', prediction_time) AS minute, COUNT(*) AS predictions,
               AVG(latency_ms) / 1000 AS latence_s
        FROM predictions WHERE prediction_time > now() - interval '30 minutes'
        GROUP BY 1 ORDER BY 1""")
    if not flow.empty:
        st.subheader("Débit et délai (30 dernières minutes)")
        a, b = st.columns(2)
        a.bar_chart(flow, x="minute", y="predictions")
        b.line_chart(flow, x="minute", y="latence_s")


with live:
    live_view()

with batch_tab:
    quality = query("SELECT metric, value, description FROM kpi_data_quality ORDER BY metric")
    if not quality.empty:
        values = dict(zip(quality.metric, quality.value))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Observations uniques", f"{int(values.get('silver_observations', 0)):,}".replace(",", " "))
        c2.metric("Doublons retirés", f"{int(values.get('duplicates_removed', 0)):,}".replace(",", " "))
        c3.metric("Navires", int(values.get("vessels", 0)))
        c4.metric("Escales", int(values.get("voyages", 0)))
    daily = query("SELECT * FROM kpi_daily_traffic ORDER BY day")
    if not daily.empty:
        st.subheader("Trafic quotidien")
        a, b = st.columns(2)
        a.line_chart(daily, x="day", y="vessels")
        b.line_chart(daily, x="day", y="avg_speed_knots")
    arrivals = query("SELECT day, destination, arrivals FROM kpi_daily_arrivals ORDER BY day")
    if not arrivals.empty:
        st.subheader("Arrivées réelles par jour et par port")
        st.bar_chart(arrivals.pivot_table(index="day", columns="destination", values="arrivals", fill_value=0))
    a, b = st.columns(2)
    with a:
        st.subheader("Par port")
        st.dataframe(query("SELECT * FROM kpi_destination"), hide_index=True, use_container_width=True)
    with b:
        st.subheader("Par catégorie de navire")
        st.dataframe(query("SELECT * FROM kpi_category"), hide_index=True, use_container_width=True)
    hourly = query("SELECT * FROM kpi_hourly_traffic ORDER BY hour_utc")
    if not hourly.empty:
        st.subheader("Observations par heure (UTC)")
        st.bar_chart(hourly, x="hour_utc", y="observations")
    if not quality.empty:
        st.subheader("Qualité des données")
        st.dataframe(quality, hide_index=True, use_container_width=True)

with model_tab:
    metrics = query("""
        SELECT model_name, phase, ROUND(mae_hours::numeric, 2) AS mae_h, ROUND(rmse_hours::numeric, 2) AS rmse_h,
               ROUND(r2::numeric, 3) AS r2, params, model_version
        FROM ml_metrics WHERE model_version = (SELECT MAX(model_version) FROM ml_metrics)
        ORDER BY phase DESC, rmse_hours""")
    if metrics.empty:
        st.info("Aucun modèle entraîné : lancez batch/train_model.py (voir README).")
    else:
        st.caption(f"Version : {metrics.model_version[0]}. Validation = choix et réglage ; "
                   "test = escales les plus récentes, jamais vues pendant l'entraînement.")
        comparison = metrics[~metrics.model_name.str.contains("tuning")]
        st.subheader("Comparaison (MAE et RMSE en heures)")
        st.bar_chart(comparison.assign(modele=comparison.model_name + " · " + comparison.phase),
                     x="modele", y=["mae_h", "rmse_h"], stack=False)
        st.dataframe(metrics.drop(columns=["model_version"]), hide_index=True, use_container_width=True)
        importance = query("""
            SELECT feature, importance FROM ml_feature_importance
            WHERE model_version = (SELECT MAX(model_version) FROM ml_feature_importance)
            ORDER BY importance DESC LIMIT 15""")
        if not importance.empty:
            st.subheader("Variables les plus utiles")
            st.bar_chart(importance, x="feature", y="importance", horizontal=True)
