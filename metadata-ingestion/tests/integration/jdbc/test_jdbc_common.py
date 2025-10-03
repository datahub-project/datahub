import logging
import subprocess
from pathlib import Path
from typing import Callable

import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def prepare_config_file(source_config: Path, tmp_path: Path, database: str) -> Path:
    """Copy and modify config file to use temporary directory."""
    with open(source_config) as f:
        config = yaml.safe_load(f)

    if "sink" in config:
        config["sink"]["config"]["filename"] = str(tmp_path / f"{database}_mces.json")

    tmp_config = tmp_path / source_config.name
    with open(tmp_config, "w") as f:
        yaml.dump(config, f)

    return tmp_config


def is_database_up(container_name: str, ready_message: str) -> bool:
    """Generic function to check if database is up using docker logs."""
    cmd = f"docker logs {container_name} 2>&1 | grep '{ready_message}'"
    ret = subprocess.run(cmd, shell=True)
    return ret.returncode == 0


def get_db_container_checker(
    container_name: str, ready_message: str
) -> Callable[[], bool]:
    """Returns a checker function for the specific database."""
    return lambda: is_database_up(container_name, ready_message)
