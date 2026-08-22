import re
from pyspark.sql import DataFrame, functions as F, types as T

_TYPE_MAP = {
    "string": T.StringType(),
    "integer": T.IntegerType(),
    "timestamp": T.TimestampType(),
}


def transform(df: DataFrame, table_cfg: dict) -> DataFrame:
    columns_cfg = table_cfg["columns"]

    # RENAME KOLOM SOURCE ke KOLOM TARGET yang ada di YAML
    for col in columns_cfg:
        if col["source"] != col["name"]:
            df = df.withColumnRenamed(col["source"], col["name"])

    # 2 & 3. null_markers then trim, per column
    for col in columns_cfg:
        name = col["name"]
        cleansing = col.get("cleansing", {})
        if name not in df.columns:
            continue
        if cleansing.get("trim"):
            df = df.withColumn(name, F.trim(F.col(name)))
        null_markers = cleansing.get("null_markers")
        if null_markers:
            df = df.withColumn(
                name,
                F.when(F.col(name).isin(null_markers), None).otherwise(F.col(name)),
            )

    # 4. type cast — after null/trim, before mapping/special_char/numeric_only
    for col in columns_cfg:
        name = col["name"]
        if name in df.columns and col["type"] in _TYPE_MAP:
            df = df.withColumn(name, F.col(name).cast(_TYPE_MAP[col["type"]]))

    # 5. value-dependent cleansing steps
    for col in columns_cfg:
        name = col["name"]
        cleansing = col.get("cleansing", {})
        if name not in df.columns:
            continue

        if "mapping" in cleansing:
            df = _apply_mapping(df, name, cleansing["mapping"])

        if "special_char" in cleansing:
            df = _apply_special_char(df, name, cleansing["special_char"])

        if "numeric_only" in cleansing:
            df = _apply_numeric_only(df, name, cleansing["numeric_only"])

    # 6. derived columns (e.g. CFVTIMESTAMP = combine(CFVDT6, CFVTME))
    for dcol in table_cfg.get("derived_columns", []):
        df = _apply_derived_column(df, dcol)

    return df


def _apply_mapping(df: DataFrame, name: str, mapping: dict) -> DataFrame:
    default = mapping.get("default")
    mapping_expr = F.create_map(
        [F.lit(x) for pair in mapping.items() if pair[0] != "default" for x in pair]
    )
    return df.withColumn(
        name,
        F.coalesce(mapping_expr[F.col(name)], F.lit(None if default in (None, "None") else default)),
    )


def _apply_special_char(df: DataFrame, name: str, spec: dict) -> DataFrame:
    lookup_col = spec["lookup_column"]
    default_rule = next((r for r in spec["rules"] if r.get("default")), None)
    df_out = df
    for rule in spec["rules"]:
        if rule.get("default"):
            continue
        # re.escape each char individually — allowed_char lists include
        # regex metacharacters (\, -, |, *) that must not be interpreted
        # as character-class syntax.
        allowed = "".join(re.escape(c) for c in rule["allowed_char"])
        allowed_pattern = rf"[^A-Za-z0-9\s{allowed}]"
        df_out = df_out.withColumn(
            name,
            F.when(
                F.col(lookup_col).isin(rule["values"]),
                F.regexp_replace(F.col(name), allowed_pattern, ""),
            ).otherwise(F.col(name)),
        )
    if default_rule:
        allowed = "".join(re.escape(c) for c in default_rule["allowed_char"])
        allowed_pattern = rf"[^A-Za-z0-9\s{allowed}]"
        matched_values = [v for r in spec["rules"] if not r.get("default") for v in r["values"]]
        df_out = df_out.withColumn(
            name,
            F.when(
                ~F.col(lookup_col).isin(matched_values),
                F.regexp_replace(F.col(name), allowed_pattern, ""),
            ).otherwise(F.col(name)),
        )
    return df_out


def _apply_numeric_only(df: DataFrame, name: str, spec: dict) -> DataFrame:
    lookup_col = spec["lookup_column"]
    return df.withColumn(
        name,
        F.when(
            F.col(lookup_col).isin(spec["values"]),
            F.regexp_replace(F.col(name), r"[^0-9]", ""),
        ).otherwise(F.col(name)),
    )


def _apply_derived_column(df: DataFrame, dcol: dict) -> DataFrame:
    if "combine" in dcol:
        combine = dcol["combine"]
        date_col, date_fmt = combine["date_column"], combine["date_format"]
        time_col, time_fmt = combine["time_column"], combine["time_format"]
        combined_str = F.concat_ws(" ", F.col(date_col).cast("string"), F.col(time_col).cast("string"))
        # spark_date_format built from the yaml date/time formats — adjust
        # ddMMyy/HHmmss -> Spark's own pattern tokens if they ever diverge.
        df = df.withColumn(
            dcol["name"],
            F.to_timestamp(combined_str, f"{date_fmt} {time_fmt}"),
        )
    return df
