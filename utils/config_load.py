import yaml

path = "/Workspace/Users/joshua.purwadi@metrodata.co.id/dbx-pipeline-integration"

def get_table_config_path(table_name: str) -> str:
    return f"{path}/config/table_{table_name}.yaml"

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def load_table_config(table_name: str) -> dict:
    path = get_table_config_path(table_name)
    return load_config(path)

def get_jdbc_options(jdbc_cfg: dict) -> dict:
    options = {
        "url": (
            f"jdbc:postgresql://"
            f"{jdbc_cfg['host']}:{jdbc_cfg['port']}/"
            f"{jdbc_cfg['database']}?"
            f"sslmode={jdbc_cfg['ssl_mode']}"
        ),
        "dbtable": jdbc_cfg["table"],
        "driver": jdbc_cfg["driver"],
        "user": dbutils.secrets.get(
            scope=jdbc_cfg["secret_scope"],
            key=jdbc_cfg["secret_key_user"]
        ),
        "password": dbutils.secrets.get(
            scope=jdbc_cfg["secret_scope"],
            key=jdbc_cfg["secret_key_password"]
        )
    }
    ## Jika ada tambahan parameter untuk Partitioning
    partition_opts = ["fetchsize", "partitionColumn", "lowerBound", "upperBound", "numPartitions"]
    for opt in partition_opts:
        if jdbc_cfg.get(opt) is not None:
            options[opt] = str(jdbc_cfg[opt])

    return options