from pyspark.sql import Window
from pyspark.sql import functions as F


def deduplicate(df, primary_keys, incremental_cols=None):

    if incremental_cols:
        order_cols = [F.col(c).desc_nulls_last() for c in incremental_cols]
        window_spec = Window.partitionBy(*primary_keys).orderBy(*order_cols)
    else:
        window_spec = Window.partitionBy(*primary_keys)

    df = (
        df
        .withColumn("_row_number", F.row_number().over(window_spec))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    return df