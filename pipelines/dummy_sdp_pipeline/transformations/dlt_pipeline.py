import dlt
from pyspark.sql.functions import col

# 1. BRONZE LAYER: Automatically stream incoming files from our mock folder
@dlt.table(
    name="dummy_bronze_orders",
    comment="Raw streaming orders ingested from cloud files location"
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Workspace/Users/adam.nevriyanto@mii.co.id/dbx-pipeline-integration/tmp/adam_retail_source/")
    )

# 2. SILVER LAYER: Enforce data quality. Drop records with missing customer IDs.
@dlt.table(
    name="dummy_silver_orders_cleaned",
    comment="Cleaned order records with valid customer identifiers"
)
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def silver_orders_cleaned():
    return (
        dlt.read_stream("dummy_bronze_orders")
        .select(
            col("order_id"),
            col("customer_id"),
            col("amount").cast("double"),
            col("order_date").cast("date")
        )
    )

# 3. GOLD LAYER: Aggregate sales totals by date for executive reporting
@dlt.table(
    name="dummy_gold_daily_sales_summary",
    comment="Business level aggregation of total sales amounts per day"
)
def gold_daily_sales_summary():
    return (
        dlt.read("dummy_silver_orders_cleaned")
        .groupBy("order_date")
        .sum("amount")
        .withColumnRenamed("sum(amount)", "total_revenue")
    )