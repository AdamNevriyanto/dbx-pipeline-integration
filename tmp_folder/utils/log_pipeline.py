import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime


def get_logger(job_name: str) -> logging.Logger:
    logger = logging.getLogger(job_name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            f"%(asctime)s | {job_name} | %(levelname)s | %(message)s"
        ))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def get_bronze_row_count(spark: SparkSession, table_name: str) -> int:
    history = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .where("operation IN ('WRITE', 'STREAMING UPDATE')")
        .orderBy(F.col("version").desc())
        .limit(1)
    )
    rows = history.collect()
    if not rows:
        return 0
    metrics = rows[0]["operationMetrics"] or {}
    return int(metrics.get("numOutputRows", 0))


def get_silver_merge_counts(spark: SparkSession, table_name: str) -> dict:
    history = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .where("operation = 'MERGE'")
        .orderBy(F.col("version").desc())
        .limit(1)
    )
    rows = history.collect()
    if not rows:
        return {"inserted": 0, "updated": 0, "deleted": 0, "total_affected": 0}

    metrics = rows[0]["operationMetrics"] or {}
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    updated = int(metrics.get("numTargetRowsUpdated", 0))
    deleted = int(metrics.get("numTargetRowsDeleted", 0))
    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "total_affected": inserted + updated + deleted,
    }


def log_job(
    spark: SparkSession,
    dbutils,
    log_table_name: str,
    source_table_name: str,
    run_id: str,
    job_name: str,
    status: str,
    start_time: datetime,
    end_time: datetime,
    duration: float,
    row_count: int | None = None,
    message: str | None = None,
):
    # Databricks metadata
    try:
        cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    except Exception:
        cluster_id = None

    try:
        job_id = spark.conf.get("spark.databricks.job.id")
    except Exception:
        job_id = None

    try:
        task_key = spark.conf.get("spark.databricks.job.taskKey")
    except Exception:
        task_key = None

    try:
        user_name = spark.sql("SELECT current_user()").first()[0]
    except Exception:
        user_name = None

    if dbutils:
        try:
            notebook_path = (
                dbutils.notebook.entry_point
                .getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
        except Exception:
            notebook_path = None
    else:
        notebook_path = None

    trigger_type = "manual" if job_id is None else "scheduled"

    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_table_name", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("task_key", StringType(), True),
        StructField("status", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("row_count", LongType(), True),
        StructField("cluster_id", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("trigger_type", StringType(), True),
        StructField("notebook_path", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("message", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ])

    log_data = [(
        run_id,
        source_table_name,
        job_name,
        task_key,
        status,
        start_time,
        end_time,
        duration,
        row_count,
        cluster_id,
        job_id,
        trigger_type,
        notebook_path,
        user_name,
        message,
        datetime.now(),
    )]

    df = spark.createDataFrame(log_data, schema)

    (
        df.write
        .mode("append")
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(log_table_name)
    )