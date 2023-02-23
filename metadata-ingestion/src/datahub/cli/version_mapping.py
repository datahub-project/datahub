
import json
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Dict, List

import yaml

class QuickstartVersionMapping(BaseModel):
    composefile_git_ref: str
    docker_tag: str

class StableVersions(BaseModel):
    force: bool
    composefile_git_ref: str
    docker_tag: str

class QuickstartChecks(BaseModel):
    valid_until_git_ref: str
    required_containers: List[str]
    ensure_exit_success: List[str]

class QuickstartVersionMappingConfig(BaseModel):
    quickstart_version_mappings: Dict[str, QuickstartVersionMapping]
    stable_versions: StableVersions
    quickstart_checks: List[QuickstartChecks]

@click.group()
def test():
    pass

@test.command()
def qs_test():
    config: QuickstartVersionMappingConfig
    
    with open("/Users/szalai1/workspace/datahub/docker/quickstart/quickstart_version_mapping.yaml") as f:
        config_raw = yaml.safe_load(f)
        click.echo(config_raw)
        config = QuickstartVersionMappingConfig.parse_obj(config_raw)

    print(config)

