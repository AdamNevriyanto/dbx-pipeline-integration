from pyspark.sql import functions as F
from pyspark.sql.window import Window


def row_hash(df, exclude_cols, hash_col="row_hash"):

    compare_cols = [c for c in df.columns if c not in exclude_cols]

    return df.withColumn(
        hash_col,
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in compare_cols]
            ),
            256
        )
    )


def cdc_flag(spark, new_df, table_name, primary_keys, hash_col="row_hash",
                    timestamp_col="ingested_timestamp", flag_col="cdc_flag"):
 
    if not spark.catalog.tableExists(table_name):
        return new_df.withColumn(flag_col, F.lit("I"))

    existing = spark.read.table(table_name)
    window = Window.partitionBy(*primary_keys).orderBy(F.col(timestamp_col).desc())

    latest_existing = (
        existing
        .withColumn("rn", F.row_number().over(window))
        .filter("rn = 1")
        .drop("rn")
        .filter(F.col(flag_col) != "D")
        .select(*primary_keys, hash_col)
    )

    new_aliased = new_df.alias("new")
    old_aliased = latest_existing.alias("old")
    join_cond = [F.col(f"new.{k}") == F.col(f"old.{k}") for k in primary_keys]

    joined = new_aliased.join(old_aliased, join_cond, "full_outer")

    flagged = joined.withColumn(
        flag_col,
        F.when(F.col(f"old.{primary_keys[0]}").isNull(), F.lit("I"))
         .when(F.col(f"new.{primary_keys[0]}").isNull(), F.lit("D"))
         .when(F.col(f"new.{hash_col}") != F.col(f"old.{hash_col}"), F.lit("U"))
         .otherwise(F.lit(None))
    )

    result_cols = [
        F.coalesce(F.col(f"new.{c}"), F.col(f"old.{c}")).alias(c)
        for c in new_df.columns if c != flag_col
    ]
    result = flagged.select(*result_cols, flag_col)

    return result.filter(F.col(flag_col).isNotNull())
