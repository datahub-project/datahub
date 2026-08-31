from dataclasses import dataclass
from typing import List, Optional

from datahub.utilities.str_enum import StrEnum

"""
Models for dbt Cloud APIs (Ref: https://docs.getdbt.com/dbt-cloud/api-v2#/)
Note: These are not complete models - they are used for data validation of reponses. In the future, we can add more fields as needed.
"""


@dataclass
class DBTCloudJob:
    id: int
    generate_docs: bool


class DBTCloudDeploymentType(StrEnum):
    PRODUCTION = "production"
    STAGING = "staging"


@dataclass
class DBTCloudEnvironment:
    id: int
    deployment_type: DBTCloudDeploymentType


@dataclass
class DBTCloudAutoDiscoveryResult:
    project_id: int
    platform_instance: str
    target_platform: str
    target_platform_instance: Optional[str]
    jobs_ids: List[int]
