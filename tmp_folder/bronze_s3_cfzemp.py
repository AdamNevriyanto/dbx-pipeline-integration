import dlt
from pyspark.sql import functions as F
sys.path.insert(0, "/Workspace/Users/jeremi.santoso@metrodata.co.id/dbx-pipeline-integration/")
from config.config_load import load_table_config

TABLE_NAME = "employee"
config = load_table_config(TABLE_NAME)
bronze_cfg = config["tables"][TABLE_NAME]["bronze"]
s3_cfg = bronze_cfg["s3"]

@dlt.table(
    name=f"bronze_s3_{TABLE_NAME}",
    partition_cols=[bronze_cfg["partition_by_col"]] if bronze_cfg.get("partition_by_col") else None
)
def bronze_customer():
    df_raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", s3_cfg["format"])
        .options(**s3_cfg["read_options"])
        .load(s3_cfg["s3_path"])
    )

    return (
        df_raw
        .withColumn("ingested_timestamp", F.current_timestamp())
        .withColumn("ingested_date", F.current_date())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )