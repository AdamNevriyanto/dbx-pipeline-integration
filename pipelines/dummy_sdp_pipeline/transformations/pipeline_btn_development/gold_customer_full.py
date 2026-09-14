import dlt
import sys
sys.path.insert(0, "/Workspace/Users/jeremi.santoso@metrodata.co.id/dbx-pipeline-integration")
from utils.config_load import load_gold_config
from utils.gold.gold_customer_join import build_gold_query

GOLD_NAME = "customer_360"
gold_cfg = load_gold_config(GOLD_NAME)


@dlt.table(name=gold_cfg["gold_table"])
def customer_360():
    return spark.sql(build_gold_query(gold_cfg))