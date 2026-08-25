import dlt
import sys
from pyspark.sql import functions as F
sys.path.insert(0, "/Workspace/Users/jeremi.santoso@metrodata.co.id/dbx-pipeline-integration/")
from utils.config_load import load_table_config
from utils.sdp_silver_transform import transform

TABLE_NAME = "customer"
config = load_table_config(TABLE_NAME)
table_cfg = config["tables"][TABLE_NAME]
bronze_cfg = table_cfg["bronze"]
silver_cfg = table_cfg["silver"]
s3_cfg = bronze_cfg["s3"]


#####################################
##### ---- BRONZE LAYER ----- #######
#####################################

@dlt.table(
    name=f"bronze_s3_{TABLE_NAME}",
    cluster_by= [bronze_cfg["cluster_by_col"]] if bronze_cfg.get("cluster_by_col") else None,
    table_properties={
    "delta.dataSkippingStatsColumns": bronze_cfg.get("cluster_by_col")
    }
)

def bronze_customer():
    df_raw = (
        spark.readStream.format("cloudFiles")
        .options(**s3_cfg["read_options"])
        .load(s3_cfg["s3_path"])
    )

    return (
        df_raw
        .withColumn("ingested_timestamp", F.current_timestamp())
        .withColumn("ingested_date", F.current_date())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )

#####################################
##### ---- SILVER LAYER ----- #######
#####################################


## STORE TRANSFORM TO VIEW
@dlt.view(name=f"silver_{TABLE_NAME}_staging")
## FILTER PK tidak boleh NULL untuk kebutuhan merging di akhir.
@dlt.expect_all_or_drop({f"valid_{pk}": f"`{pk}` IS NOT NULL" for pk in silver_cfg["primary_keys"]})
def staged():
    df_bronze = dlt.read_stream(f"bronze_s3_{TABLE_NAME}")
    return transform(df_bronze, table_cfg)


dlt.create_streaming_table(
    name=f"silver_s3_{TABLE_NAME}",
    cluster_by=[silver_cfg["partition_columns"]] if silver_cfg.get("partition_columns") else None,
    table_properties={
    "delta.dataSkippingStatsColumns": silver_cfg.get("partition_columns")
    }
    )

## Logic Untuk Ambil Data Terbaru 
if silver_cfg.get("incremental_columns"):
    sequence_by = F.struct(
        F.col(silver_cfg["incremental_columns"][0]),
        F.col("ingested_timestamp"),
    )
else:
    sequence_by = F.col("ingested_timestamp")

dlt.apply_changes(
    target=f"silver_s3_{TABLE_NAME}",
    source=f"silver_{TABLE_NAME}_staging",
    keys=silver_cfg["primary_keys"],
    sequence_by=sequence_by,
    stored_as_scd_type=1
    )