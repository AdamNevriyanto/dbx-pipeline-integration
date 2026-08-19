from delta.tables import DeltaTable

def write_udf(
    spark,
    df,
    mode,
    table_name: str,
    path: str = None,
    partition_cols: list = None,
    format: str = "delta",
    merge_schema: bool = True
):

    writer = (
        df.write
        .format(format)
        .mode(mode)
    )

    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    table_exists = spark.catalog.tableExists(table_name)

    # Apply partition only when creating table
    if partition_cols and not table_exists:
        writer = writer.partitionBy(*partition_cols)

    # Set external path only during table creation
    if path and not table_exists:
        writer = writer.option("path", path)

    writer.saveAsTable(table_name)


def merge_silver(
    spark,
    df,
    table_name,
    primary_keys,
    partition_cols=None
):
    
    if not spark.catalog.tableExists(table_name):
        writer = (
            df.write
            .format("delta")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.saveAsTable(table_name)

    else:

        if not primary_keys:
            raise ValueError("primary_keys cannot be empty for MERGE operation")

        source_alias="s"
        target_alias="t"

        conditions = [
            f"{source_alias}.{col} <=> {target_alias}.{col}"
            for col in primary_keys
        ]
        merge_condition = " AND ".join(conditions)

        deltaTable = DeltaTable.forName(spark, table_name)
        (
            deltaTable.alias(target_alias)
            .merge(
                df.alias(source_alias), 
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
