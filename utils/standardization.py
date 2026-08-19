import re
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, trim, upper, lpad, concat, to_timestamp, expr, try_to_timestamp

#Standarisasi untuk Replace value, contoh untuk gender
def std_category(df, column_name, mapping: dict, default=None):
    col_expr = when(upper(trim(col(column_name))) == list(mapping.keys())[0], list(mapping.values())[0])
    for k, v in list(mapping.items())[1:]:
        col_expr = col_expr.when(upper(trim(col(column_name))) == k, v)
    col_expr = col_expr.otherwise(default)
    return df.withColumn(column_name, col_expr)


#CLEANSING untuk kolom yang boleh numeric saja
def std_numeric_only(df, column_name: str, lookup_column: str = None, values: list = None):
    cleaned_expr = F.regexp_replace(F.col(column_name), r"[^0-9]", "")

    if lookup_column and values:
        condition = F.col(lookup_column).isin(values)
        final_expr = F.when(condition, cleaned_expr).otherwise(F.col(column_name))
    else:
        final_expr = cleaned_expr

    df = df.withColumn(column_name, final_expr)

    # kosongkan string hasil strip yang jadi "" -> NULL
    df = df.withColumn(
        column_name,
        F.when(F.col(column_name) == "", None).otherwise(F.col(column_name))
    )

    return df

## STANDARISASI SPESIAL KARAKTER
def _build_allowed_regex(extra_chars: list) -> str:
    escaped = "".join(re.escape(c) for c in extra_chars)
    return r"[^A-Za-z0-9\s" + escaped + r"]"


def std_allowed_chars(df, column_name: str, condition_column: str, rules: list):
    default_regex = None
    expr = None

    # DEFAULT RULE, skip jika tidak diisi di yaml
    for rule in rules:
        if rule.get("default"):
            default_regex = _build_allowed_regex(rule["allowed_char"])

    # CARI PATTERN REGEX SPESIAL KARAKTER YANG BUKAN DEFAULT
    for rule in rules:
        if rule.get("default"):
            continue
        regex = _build_allowed_regex(rule["allowed_char"])
        condition = F.col(condition_column).isin(rule["values"])
        cleaned = F.regexp_replace(F.col(column_name), regex, "")
        expr = F.when(condition, cleaned) if expr is None else expr.when(condition, cleaned)

    if default_regex:
        default_cleaned = F.regexp_replace(F.col(column_name), default_regex, "")
        expr = expr.otherwise(default_cleaned) if expr is not None else default_cleaned
    else:
        expr = expr.otherwise(F.col(column_name)) if expr is not None else F.col(column_name)

    df = df.withColumn(column_name, expr)
    return df

## Standarisasi kolom date + time 
def std_date(df, table_config: dict):
    derived_columns = table_config.get("derived_columns", [])

    for derived in derived_columns:
        col_name = derived["name"]
        combine_cfg = derived.get("combine")

        if combine_cfg:
            date_col = combine_cfg["date_column"]
            time_col = combine_cfg["time_column"]
            date_fmt = combine_cfg.get("date_format", "ddMMyy")
            time_fmt = combine_cfg.get("time_format", "HHmmss")

            # pad biar length-nya konsisten (DDMMYY = 6 digit, HHMMSS = 6 digit)
            date_str = lpad(col(date_col).cast("string"),len(date_fmt), "0")
            time_str = lpad(col(time_col).cast("string"),len(time_fmt), "0")

            # combined_str = concat(date_str, time_str)
            combined_fmt = date_fmt + time_fmt
            df = df.withColumn("_combined_str_tmp", concat(date_str, time_str))

            df = df.withColumn(
                col_name,
                F.expr(f"try_to_timestamp(_combined_str_tmp, '{combined_fmt}')")
            )
            
            df = df.drop("_combined_str_tmp")

    return df

## CONVERT JULIAN DATE
def std_julian_date(df, column_name: str, output_column: str = None):
    output_column = output_column or column_name
    col_str = F.col(column_name).cast("string")

    year        = F.substring(col_str, 1, 4).cast("int")
    day_of_year = F.substring(col_str, 5, 3).cast("int")

    base_date = F.to_date(F.concat(year.cast("string"), F.lit("-01-01")))
    result = F.date_add(base_date, (day_of_year - 1).cast("int"))

    # kalau source-nya null/0/kosong, hasil jadi NULL, bukan error
    df = df.withColumn(
        output_column,
        F.when(
            (F.col(column_name).isNull()) | (F.col(column_name) == 0),
            None
        ).otherwise(result)
    )
    return df