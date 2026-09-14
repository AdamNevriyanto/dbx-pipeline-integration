def build_select_clause(select_columns: list) -> str:
    parts = []
    for sel in select_columns:
        src = sel["source"]
        for col in sel["columns"]:
            parts.append(f"{src}.{col}")
    return ",\n  ".join(parts)


## BUAT CTE UNTUK RANKING DAN AMBIL DATA TERBARU DARI RELASI 1 to MANY dari CUSTOMER
def build_dedup_ctes(sources: dict) -> tuple[str, dict]:
    ctes = []
    table_refs = {}

    for alias, src in sources.items():
        if src.get("ranking"):
            rank = src["ranking"]
            cte_name = f"{alias}_latest"
            direction = rank.get("order_by", "DESC")

            ctes.append(f"""
    {cte_name} AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {rank['partition_by']}
                ORDER BY {rank['order_columns']} {direction}
            ) AS rn
        FROM {src['table']}
    )""")
            table_refs[alias] = cte_name
        else:
            table_refs[alias] = src["table"]

    cte_clause = ",\n".join(ctes)
    return cte_clause, table_refs

def build_join_clause(joins: list, sources: dict) -> str:
    clauses = []
    for j in joins:
        right_alias = j["right"]
        right_table = sources[right_alias]["table"]
        join_type = j.get("type", "left").upper()

        conditions = " AND ".join([
            f"{j['left']}.{cond['left_col']} = {right_alias}.{cond['right_col']}"
            for cond in j["on"]
        ])

        ## FILTER RN = 1 (LATEST DATA)
        if sources[right_alias].get("ranking"):
            conditions += f" AND {right_alias}.rn = 1"

        clauses.append(f"{join_type} JOIN {right_table} AS {right_alias} ON {conditions}")
    return "\n".join(clauses)


def build_gold_query(gold_cfg: dict) -> str:
    """Bangun full SQL query gold table dari config YAML."""
    sources = gold_cfg["sources"]
    joins = gold_cfg["joins"]
    select_columns = gold_cfg["select_columns"]

    cte_clause, table_refs = build_dedup_ctes(sources)

    base_alias = joins[0]["left"]
    base_table = sources[base_alias]["table"]

    select_clause = build_select_clause(select_columns)
    join_clause = build_join_clause(joins, sources)

    with_clause = f"WITH{cte_clause}" if cte_clause else ""
    return f"""
        {with_clause}
        SELECT
          {select_clause}
        FROM {base_table} AS {base_alias}
        {join_clause}
    """