def write_bronze(spark, df, table_name, partition_by_col, primary_keys, run_optimize=True):
    df.write.format("delta") \
        .mode("append") \
        .partitionBy(partition_by_col) \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)

    if run_optimize:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({', '.join(primary_keys)})")
