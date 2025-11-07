"""
Configuration loader for path locations and future pipeline settings.

Having a central config reader:
- Makes paths & settings environment-agnostic
- Eases the future switch from local FS to DBFS/S3
- Keeps ETL code free of hard-coded strings
"""
import yaml
from typing import Dict

def load_path_config() -> Dict[str, str]:
    with open("config.yml", "r") as f:
        cfg = yaml.safe_load(f)
    return cfg["paths"]
