import dlt
import sys
sys.path.insert(0, "/Workspace/Users/jeremi.santoso@metrodata.co.id/dbx-pipeline-integration/")

from pyspark.sql import functions as F
from config.config_load import load_table_config
from utils.sdp_silver_transform import transform

# DEFINE NAMA TABLE
TABLE_NAME = "customer"
# CONFIG_PATH = f"/Workspace/Users/jeremi.santoso@metrodata.co.id/dbx-pipeline-integration/config/table_{TABLE_NAME}.yaml"
config = load_table_config(TABLE_NAME)
table_cfg = config["tables"][TABLE_NAME]
silver_cfg = table_cfg["silver"]

@dlt.view(name=f"silver_{TABLE_NAME}_staging")
def staged():
    df_bronze = dlt.read_stream(f"bronze_s3_{TABLE_NAME}")
    df_transform = transform(
        df_bronze,
        table_cfg
        )
    ## FILTER PK tidak boleh NULL untuk kebutuhan merging di akhir.
    return df_transform.filter(
        " AND ".join([f"{pk} IS NOT NULL" for pk in silver_cfg["primary_keys"]])
    )


dlt.create_streaming_table(
    name=f"silver_s3_{TABLE_NAME}",
    partition_cols=silver_cfg.get("partition_columns")
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