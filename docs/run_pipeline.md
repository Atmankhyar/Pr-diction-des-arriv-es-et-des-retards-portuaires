# Pipeline Execution Guide / Guide d'exécution du pipeline

This guide explains how to run the end-to-end Big Data pipeline for port arrival and delay prediction: from cluster deployment to live real-time streaming and monitoring.

---

## Architecture Flow Overview

```
                                  ┌──────────────────────────┐
                                  │   Raw CSV (AIS2ETA)      │
                                  └─────────────┬────────────┘
                                                │ prepare.py
                                                ▼
                                  ┌──────────────────────────┐
                                  │   Kafka Broker (KRaft)   │
                                  └──────┬────────────┬──────┘
             maritime.observations.v1    │            │ maritime.labels.v1
                                         │            │
                  ┌──────────────────────┘            └──────────────────────┐
                  ▼                                                          ▼
        ┌───────────────────┐                                      ┌───────────────────┐
        │   stream-archive  │ (Spark Structured Streaming)         │   stream-predict  │ (Real-time inference)
        └─────────┬─────────┘                                      └─────────┬─────────┘
                  │                                                          │
                  ▼                                                          ▼
        ┌───────────────────┐                                      ┌───────────────────┐
        │   HDFS (Parquet)  │                                      │    PostgreSQL     │
        │  Bronze / Silver  │                                      │ (predictions/kpi) │
        └─────────┬─────────┘                                      └─────────┬─────────┘
                  │                                                          │
          ┌───────┴───────┐                                                  ▼
          ▼               ▼                                        ┌───────────────────┐
    indicators.py   train_model.py                                 │ Streamlit Web App │
     (Spark SQL)      (Spark ML)                                   │ (Port 8501)       │
          │               │                                        └───────────────────┘
          ▼               ▼
     PostgreSQL       HDFS Models (/models/eta/current)
```

---

## Prerequisites & Requirements

- **Operating System:** Linux, macOS, or Windows with WSL2.
- **Docker Engine & Docker Compose v2**: Daemon must be running.
- **Memory:** Minimum 4 GB RAM allocated to Docker (8 GB recommended).
- **Disk Space:** ~8 GB free disk space (images ~4 GB, Kafka & HDFS storage ~2 GB).

---

## Step 0: Troubleshooting Port Conflicts (e.g., Port 8081)

If you see an error such as:
```text
Bind for 0.0.0.0:8081 failed: port is already allocated
```

This happens when a local daemon, another container, or a system service on your machine is already using port `8081` (the default Spark Worker 1 web UI port).

### Solution Option A: Change the worker port in `.env` (Recommended)
Edit `.env` and assign an unused host port (such as `18081`):
```bash
# In .env:
SPARK_WORKER_1_PORT=18081
SPARK_WORKER_2_PORT=18082
```
Then restart the cluster:
```bash
docker compose up -d
```

### Solution Option B: Find and stop the conflicting process
On Linux:
```bash
# Identify which process is listening on port 8081
sudo lsof -i :8081 || sudo ss -tulpn | grep 8081

# If it is an old docker container, stop it
docker ps --filter "publish=8081"
docker stop <container_id>
```

---

## Step 1: Start the Cluster Infrastructure

Launch all background services (Kafka, HDFS NameNode, 2 DataNodes, Spark Master, 2 Spark Workers, PostgreSQL, and Streamlit):

```bash
# 1. Create your environment configuration (if not already created)
cp -n .env.example .env

# 2. Build and launch services in detached mode
docker compose up -d --build

# 3. Check status of all containers
docker compose ps
```

### Verify HDFS Health
Confirm that Hadoop HDFS is healthy and both DataNodes are registered:
```bash
docker compose run --rm hdfs dfsadmin -report
```
*Expected output: Under "Live datanodes", you should see `2`.*

---

## Step 2: Download and Prepare AIS Data

Run the containerized ingestion tool to fetch the dataset and generate preprocessed batches:

```bash
# 1. Download the raw AIS dataset with SHA-256 validation
docker compose run --rm ingestion ingestion.download

# 2. Clean observations, convert to UTC, generate stable event IDs
docker compose run --rm ingestion ingestion.prepare --output data/prepared/full --source-timezone UTC --limit 0
```

> [!TIP]
> **Fast Trial Mode:** For quick testing or systems with limited RAM, limit the dataset size to 50,000 records:
> ```bash
> docker compose run --rm ingestion ingestion.prepare --output data/prepared/sample --source-timezone UTC --limit 50000
> ```
> If using sample data, replace `data/prepared/full` with `data/prepared/sample` in subsequent commands.

---

## Step 3: Cold Path — Stream Ingestion & HDFS Archival

Stream the prepared dataset through Kafka into HDFS (Parquet format) and audit the delivery.

```bash
# 1. Start Spark Structured Streaming archiver in background
docker compose --profile streaming up -d stream-archive

# 2. Publish observations and labels to Kafka at max throughput (--rate 0)
docker compose run --rm ingestion ingestion.producer --batch data/prepared/full --publish --include-labels --rate 0

# 3. Verify Kafka ingestion against the producer report
# (Replace <ingestion_run_id> with the actual timestamp/run ID printed by the producer)
docker compose run --rm ingestion ingestion.verify --summary data/reports/<ingestion_run_id>.summary.json

# 4. Confirm HDFS Parquet files and block replication factor (target = 2.0)
docker compose run --rm hdfs dfs -du -h /data/bronze
docker compose run --rm hdfs fsck /data/bronze

# 5. Stop the stream archiver to free memory for batch jobs
docker compose stop stream-archive
```

---

## Step 4: Batch Analytics — Spark SQL KPIs

Run the batch SQL job to deduplicate raw bronze records into silver data and calculate port performance KPIs:

```bash
docker compose run --rm batch /opt/app/batch/indicators.py
```

This populates PostgreSQL tables:
- `kpi_vessel_counts`: vessel counts by port and category.
- `kpi_average_delays`: mean and median delay per port.
- `kpi_hourly_arrivals`: peak traffic distribution across hours.
- `kpi_data_quality`: missing value counts and anomaly rates.

---

## Step 5: Train, Compare, and Save ML Models

Train baseline and machine learning models (Linear Regression, Random Forest, GBT) on historical trips:

```bash
docker compose run --rm batch /opt/app/batch/train_model.py
```

> [!NOTE]
> On machines with 4 GB RAM allocated to Docker, add `--sample 0.3` to train on a 30% sample and prevent out-of-memory errors:
> ```bash
> docker compose run --rm batch /opt/app/batch/train_model.py --sample 0.3
> ```

This job:
1. Performs a temporal train/validation/test split grouped by vessel call.
2. Compares Baseline (median by port), Linear Regression, Random Forest, and GBT.
3. Automatically tunes the best model using CrossValidator.
4. Saves the versioned `PipelineModel` to HDFS (`/models/eta/current`).
5. Persists evaluation metrics into PostgreSQL (`ml_metrics`) and `docs/results/metrics_latest.json`.

---

## Step 6: Hot Path — Real-time Prediction

Start the real-time inference streaming pipeline and replay test-period data:

```bash
# 1. Start the prediction driver
docker compose --profile streaming up -d stream-predict

# 2. Follow logs until "Modèle chargé" (Model loaded) is shown
docker compose logs -f stream-predict
```
*(Press `Ctrl+C` to exit the log follower once the model has loaded).*

Now extract and replay observations from the test cutoff date (e.g., `2020-08-25`):
```bash
# 3. Extract the live test slice
docker compose run --rm ingestion ingestion.slice_batch --batch data/prepared/full --output data/prepared/live --from-time 2020-08-25T00:00:00Z

# 4. Stream observations into Kafka at 20 events/sec
docker compose run --rm ingestion ingestion.producer --batch data/prepared/live --publish --rate 20
```

---

## Step 7: View Dashboards & Web Interfaces

Open your browser to monitor the pipeline:

| Interface | URL | Description |
|---|---|---|
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | Live vessel positions, real-time ETA predictions, historical KPIs, ML metrics |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | Running jobs, active workers, executor memory |
| **Spark Worker 1 UI** | `http://localhost:8081` (or `${SPARK_WORKER_1_PORT}`) | Worker 1 details |
| **Spark Worker 2 UI** | `http://localhost:8082` (or `${SPARK_WORKER_2_PORT}`) | Worker 2 details |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | HDFS filesystem explorer, DataNode live status |
| **PostgreSQL Database** | `localhost:55432` | DB: `port_eta`, User: `port_eta`, Password: `port_eta` |

---

## Step 8: Verify Fault Tolerance & Exactly-Once Semantics

To verify that the hot path is resilient and does not introduce duplicates when a failure occurs:

```bash
# 1. Forcibly kill the active streaming predictor mid-run
docker compose kill stream-predict

# 2. Restart the prediction streaming service
docker compose --profile streaming up -d stream-predict

# 3. Query PostgreSQL to verify that row count equals distinct event count
docker compose exec postgres psql -U port_eta -d port_eta -c "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT event_id) AS distinct_events, MAX(prediction_time) AS latest_prediction FROM predictions;"
```
*Expected: `total_rows == distinct_events` (ensured by PostgreSQL primary key and `ON CONFLICT DO NOTHING`).*

---

## Teardown & Reset Commands

```bash
# Normal shutdown (preserves all Kafka, HDFS, and PostgreSQL volumes):
docker compose --profile streaming --profile batch down

# Complete teardown and clean slate (deletes all data volumes):
docker compose --profile streaming --profile batch down -v
```
