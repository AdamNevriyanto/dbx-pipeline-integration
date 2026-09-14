# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Unified Run Log Builder
# MAGIC %md
# MAGIC # Unified Run Log Builder
# MAGIC
# MAGIC This notebook builds a single Delta table that consolidates **SDP Pipeline runs** and **Regular Job runs** into one unified log. It captures:
# MAGIC
# MAGIC | Column | Description |
# MAGIC | --- | --- |
# MAGIC | `run_type` | PIPELINE or JOB |
# MAGIC | `id` | pipeline_id or job_id |
# MAGIC | `name` | pipeline or job name |
# MAGIC | `run_id` | update_id (pipelines) or run_id (jobs) |
# MAGIC | `status` | COMPLETED / FAILED / CANCELED |
# MAGIC | `start_time` | UTC timestamp |
# MAGIC | `end_time` | UTC timestamp |
# MAGIC | `duration_seconds` | Computed duration |
# MAGIC | `total_rows_processed` | From event_log (pipelines only, NULL for jobs) |
# MAGIC
# MAGIC > **Requirements:**
# MAGIC > - Must run on a **SQL Warehouse** or **Shared cluster** (event_log() is not accessible from assigned clusters)
# MAGIC > - User needs SELECT access to `system.lakeflow.*` system tables
# MAGIC >
# MAGIC > **Tip:** Schedule this notebook as a daily job to keep the log table fresh.

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration - Set your target catalog, schema, and table name
catalog = "mii_workspace"
schema = "default"
table = "log_jobs_2"

# How many days of history to include
lookback_days = 30

PIPELINE_NAMES = [
    "pipeline_btn_development"
]

print(f"Target table: {catalog}.{schema}.{table}")
print(f"Lookback period: {lookback_days} days")
print(f"Monitoring pipelines: {PIPELINE_NAMES}")

# COMMAND ----------

# DBTITLE 1,Get all pipeline IDs
pipeline_ids_df = spark.sql(f"""
  SELECT DISTINCT pipeline_id, name
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.pipelines
  )
  WHERE rn = 1
    AND name IN ({','.join([f"'{n}'" for n in PIPELINE_NAMES])})
""")

pipeline_ids = [row.pipeline_id for row in pipeline_ids_df.collect()]
print(f"Found {len(pipeline_ids)} pipeline(s) matching nama:")
display(pipeline_ids_df)

# COMMAND ----------

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

def get_pipeline_spec(pipeline_id):
    r = requests.get(f"{host}/api/2.0/pipelines/{pipeline_id}", headers=headers)
    r.raise_for_status()
    return r.json()

# COMMAND ----------

# Cek konfigurasi event log & build mapping
pipeline_info = []

for pid in pipeline_ids:
    try:
        resp = get_pipeline_spec(pid)
        spec = resp.get("spec", resp)

        name = spec.get("name", pid)
        catalog_p = spec.get("catalog")
        schema_p = spec.get("schema")
        event_log_cfg = spec.get("event_log")

        if event_log_cfg:
            elog_catalog = event_log_cfg.get("catalog") or catalog_p
            elog_schema = event_log_cfg.get("schema") or schema_p
            elog_table = event_log_cfg.get("name", "event_log")
            full_table = f"{elog_catalog}.{elog_schema}.{elog_table}"
            status = "PUBLISHED"
        else:
            full_table = None
            status = "NOT_PUBLISHED"

        pipeline_info.append({
            "pipeline_id": pid, "name": name,
            "catalog": catalog_p, "schema": schema_p,
            "event_log_status": status, "event_log_table": full_table
        })
    except Exception as e:
        pipeline_info.append({
            "pipeline_id": pid, "name": None, "catalog": None, "schema": None,
            "event_log_status": f"ERROR: {e}", "event_log_table": None
        })

pipeline_event_log_tables = {
    p["pipeline_id"]: p["event_log_table"]
    for p in pipeline_info
    if p["event_log_table"] is not None
}

not_ready = [p for p in pipeline_info if p["event_log_table"] is None]
print(f"Pipeline siap dipakai: {len(pipeline_event_log_tables)}")
if not_ready:
    print(f"{len(not_ready)} pipeline belum published event log:")
    for p in not_ready:
        print(f"  - {p['name']} ({p['pipeline_id']}) - {p['event_log_status']}")

# COMMAND ----------

# DBTITLE 1,Get pipeline row counts from event_log
# Get per-table (per-flow) metrics from event log
from pyspark.sql import functions as F
from functools import reduce

flow_metrics_dfs = []
skipped_pipelines = []

for pid, tbl in pipeline_event_log_tables.items():
    try:
        df = spark.sql(f"""
            WITH flow_events AS (
              SELECT
                origin.pipeline_id,
                origin.update_id,
                origin.flow_name AS table_name,
                timestamp,
                details:flow_progress.status AS status,
                TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT) AS num_output_rows,
                TRY_CAST(details:flow_progress.metrics.num_upserted_rows AS BIGINT) AS num_upserted_rows,
                TRY_CAST(details:flow_progress.metrics.num_deleted_rows AS BIGINT) AS num_deleted_rows
              FROM {tbl}
              WHERE event_type = 'flow_progress'
            )
            SELECT
              pipeline_id, update_id, table_name,
              MIN(CASE WHEN status = 'STARTING' THEN timestamp END) AS start_time,
              MAX(CASE WHEN status = 'COMPLETED' THEN timestamp END) AS end_time,
              MAX(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS has_failed,
              SUM(COALESCE(num_output_rows,0) + COALESCE(num_upserted_rows,0)) AS total_rows_processed,
              SUM(COALESCE(num_deleted_rows,0)) AS total_rows_deleted
            FROM flow_events
            GROUP BY pipeline_id, update_id, table_name
        """)
        df = df.persist()
        df.count()
        flow_metrics_dfs.append(df)
    except Exception as e:
        skipped_pipelines.append((pid, str(e)))
        print(f"⚠️ Skipped pipeline {pid}: {e}")

if flow_metrics_dfs:
    flow_metrics_df = reduce(lambda a, b: a.union(b), flow_metrics_dfs)
    print(f"✅ Collected per-table metrics from {len(flow_metrics_dfs)} pipeline(s)")
else:
    flow_metrics_df = None
    print("⚠️ No per-table metrics collected")

# COMMAND ----------

# DBTITLE 1,Build unified run log DataFrame
# Build unified run log (per-table granularity)
if flow_metrics_df is not None:
    flow_metrics_df = (
        flow_metrics_df
        .withColumn("duration_seconds", F.col("end_time").cast("long") - F.col("start_time").cast("long"))
        .withColumn(
            "status",
            F.when(F.col("has_failed") == 1, "FAILED")
             .when(F.col("end_time").isNotNull(), "COMPLETED")
             .otherwise("UNKNOWN")
        )
    )

    pipeline_names_df = spark.sql("""
        SELECT pipeline_id, name FROM (
          SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
          FROM system.lakeflow.pipelines
        ) WHERE rn = 1
    """)

    final_df = (
        flow_metrics_df
        .join(pipeline_names_df, "pipeline_id", "left")
        .select(
            F.lit("PIPELINE").alias("run_type"),
            F.col("pipeline_id").alias("id"),
            F.col("name"),
            F.col("update_id").alias("run_id"),
            F.col("table_name"),
            F.col("status"),
            F.col("start_time"),
            F.col("end_time"),
            F.col("duration_seconds"),
            F.col("total_rows_processed"),
            F.col("total_rows_deleted"),
          )
        .orderBy(F.col("start_time").desc())
    )
    print(f"Unified log built: {final_df.count()} rows")
    final_df.printSchema()
else:
    final_df = None
    print("final_df kosong — cek flow_metrics_df di cell sebelumnya")

# COMMAND ----------

# DBTITLE 1,Write to Delta table
# Write to Delta table
target_table = f"{catalog}.{schema}.{table}"

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {target_table} (
    run_type STRING, id STRING, name STRING, run_id STRING, table_name STRING,
    status STRING, start_time TIMESTAMP, end_time TIMESTAMP, duration_seconds LONG,
    total_rows_processed LONG, total_rows_deleted LONG
  )
""")

final_df.createOrReplaceTempView("new_runs")

spark.sql(f"""
  MERGE INTO {target_table} AS target
  USING new_runs AS source
  ON target.id = source.id
     AND target.run_id = source.run_id
     AND target.table_name = source.table_name
  WHEN MATCHED AND source.status != target.status THEN
    UPDATE SET status = source.status, end_time = source.end_time,
      duration_seconds = source.duration_seconds,
      total_rows_processed = source.total_rows_processed,
      total_rows_deleted = source.total_rows_deleted
  WHEN NOT MATCHED THEN INSERT *
""")

print(f"MERGE completed on: {target_table}")

# COMMAND ----------

# DBTITLE 1,Verify written table
# Display a sample from the written table to confirm success
result = spark.sql(f"""
  SELECT *
  FROM {catalog}.{schema}.{table}
  ORDER BY start_time DESC
  LIMIT 20
""")

print(f"Table: {catalog}.{schema}.{table}")
print(f"Total rows: {result.count()}")
display(result)

# COMMAND ----------

# 1. Cek apakah pipeline_event_log_tables beneran keisi ID yang bener (bukan placeholder)
print("Pipeline IDs dari system table:", pipeline_ids)
print("Mapping event log tables:", pipeline_event_log_tables)
print("Overlap:", set(pipeline_ids) & set(pipeline_event_log_tables.keys()))

# COMMAND ----------

tbl = "mii_workspace.default.event_log_pipeline_btn"

spark.sql(f"""
    SELECT origin.flow_name, timestamp, 
           details:flow_progress.status AS status,
           details
    FROM {tbl}
    WHERE event_type = 'flow_progress'
      AND origin.update_id = 'f33af277-1aa7-4d5a-a6bb-8216cfb1c9b0'
      AND origin.flow_name = 'mii_workspace.default.silver_s3_customer'
    ORDER BY timestamp
""").show(truncate=False)