import yaml

path = "/Workspace/Users/jeremi.santoso@metrodata.co.id/dbx-pipeline-integration"

def get_table_config_path(table_name: str) -> str:
    return f"{path}/config/table_{table_name}.yaml"

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def load_table_config(table_name: str) -> dict:
    path = get_table_config_path(table_name)
    return load_config(path)