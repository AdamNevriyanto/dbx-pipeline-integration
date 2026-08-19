from pyspark.sql import functions as F


def get_full_table_name(catalog: str, schema: str, table_name: str) -> str:
    return f"{catalog}.{schema}.{table_name}"


def dq_check_gender_vs_nik(df, ssno_col="CFSSNO", sex_col="CFSEX"):
    substr_col = F.substring(F.col(ssno_col), 7, 2)

    df = df.withColumn("_digit_78_str", substr_col)
    df = df.withColumn(
        "_digit_78",
        F.expr("try_cast(_digit_78_str as int)")
    )

    expected_sex = F.when(F.col("_digit_78").between(1, 31), "L") \
                     .when(F.col("_digit_78").between(41, 71), "P") \
                     .otherwise(None)

    df = df.withColumn("_expected_sex_from_nik", expected_sex)
    df = df.withColumn(
        "dq_gender_nik_match",
        F.when(F.col("_expected_sex_from_nik").isNull(), None)
         .when(F.col("_expected_sex_from_nik") == F.col(sex_col), True)
         .otherwise(False)
    )

    df = df.drop("_digit_78_str", "_digit_78", "_expected_sex_from_nik")
    return df


def apply_rule_check(df, rule: dict):
    rule_type = rule["type"]

    if rule_type == "gender_nik_check":
        df_checked = dq_check_gender_vs_nik(
            df,
            ssno_col=rule["params"]["ssno_column"],
            sex_col=rule["params"]["sex_column"]
        )
        return df_checked, "dq_gender_nik_match"

    # elif rule_type == "min_value_check":
    #     df_checked = dq_check_min_value(
    #         df,
    #         column=rule["params"]["column"],
    #         min_value=rule["params"]["min_value"]
    #     )
    #     return df_checked, "_dq_check_result"

    else:
        raise ValueError(f"Unknown DQ rule type: {rule_type}")


def run_dq_rule(df, rule_name, rule_category, column_name, severity,
                 check_column, table_name, run_id, primary_key,checked_column=None):
    df_result = df.withColumn("status",
        F.when(F.col(check_column).isNull(), "NULL_SKIP")
         .when(F.col(check_column) == True, "PASS")
         .otherwise("FAIL")
    )

    checked_column = checked_column or []

    detail = [
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("check_timestamp"),
        F.lit(table_name).alias("table_name"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(rule_category).alias("rule_category"),
        F.lit(column_name).alias("column_name"),
        F.col(primary_key).cast("string").alias("CFCIF"),
        # F.lit(primary_key).alias("primary_key_column"),
        F.col("status"),
        F.lit(severity).alias("severity"),
    ]

    ## ADD KOLOM CHECKED TO DQ_PROF_TABLE
    for c in checked_column:
        if c in df.columns:
            detail.append(F.col(c).cast("string").alias(c))
    ##SELECT DATA YANG FAIL AJA BUAT DI SIMPAN DI DETAIL TABLE
    detail = df_result.filter(F.col("status") == "FAIL").select(*detail)

    summary = df_result.groupBy().agg(
        F.count("*").alias("total_rows_checked"),
        F.sum(F.when(F.col("status") == "PASS", 1).otherwise(0)).alias("total_pass"),
        F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("total_fail"),
        F.sum(F.when(F.col("status") == "NULL_SKIP", 1).otherwise(0)).alias("total_null_skip"),
    ).withColumn("pass_rate", F.col("total_pass") / F.col("total_rows_checked")) \
     .withColumn("run_id", F.lit(run_id)) \
     .withColumn("check_timestamp", F.current_timestamp()) \
     .withColumn("table_name", F.lit(table_name)) \
     .withColumn("rule_name", F.lit(rule_name)) \
     .withColumn("rule_category", F.lit(rule_category)) \
     .withColumn("severity", F.lit(severity))

    return summary, detail