#!/usr/bin/env bash
# Soumet un script PySpark au cluster en mode client.
# Le driver tourne dans ce conteneur : les executors doivent pouvoir le joindre par son IP.
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage : submit.sh <script.py> [options du script]" >&2
  exit 2
fi

DRIVER_HOST="$(hostname -i | awk '{print $1}')"

exec /opt/spark/bin/spark-submit \
  --conf "spark.driver.host=${DRIVER_HOST}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-768m}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-640m}" \
  "$@"
