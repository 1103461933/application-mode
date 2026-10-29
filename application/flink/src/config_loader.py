"""Kept separate for future environment-specific configuration adapters."""
import os
import yaml

def load_yaml(path=None):
    path = path or os.getenv("PIPELINE_CONFIG", "/opt/flink/usrlib/application/config/jobs.yaml")
    with open(path, encoding="utf-8") as stream:
        return yaml.safe_load(stream)
