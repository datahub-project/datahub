from typing import Optional, Dict

import certifi
from datahub.emitter.mcp_builder import ContainerKey
from pydantic import Field, root_validator

from datahub.configuration.common import ConfigModel, AllowDenyPattern
import logging

logger = logging.Logger(__name__)

class DremioSpaceKey(ContainerKey):
    space_name: str
    platform_instance: str


class DremioFolderKey(ContainerKey):
    folder_name: str
    parent_folder: str
    platform_instance: str

class DremioSourceConfig(ConfigModel):
    # Dremio Connection Details

    hostname: Optional[str] = Field(
        default=None,
        description="Hostname or IP Address",
    )

    port: Optional[int] = Field(
        default=9047,
        description="REST API port",
    )

    tls: Optional[bool] = Field(
        default=True,
        description="Whether the Dremio REST API port is encrypted",
    )

    disable_certificate_verification: Optional[bool] = Field(
        default=False,
        description="Disable TLS certificate verification",
    )

    username: Optional[str] = Field(
        default=None,
        description="Dremio username",
    )

    authentication_method: Optional[str] = Field(
        default="password",
        description="Is a Personal Access Token or a password used for authentication",
    )

    password: Optional[str] = Field(
        default=9047,
        description="Dremio REST API port",
    )

    path_to_certificates: str = certifi.where()

    #Datahub Environment details

    env: Optional[str] = Field(
        default="PROD",
        description="Environment to use in namespace when constructing URNs.",
    )

    platform_instance: Optional[str] = Field(
        default="",
        description="The instance of the platform that all assets produced by this recipe belong to.",
    )

    # Entity Filters

    collect_pds: bool = False
    table_allow: bool = True

    collect_system_tables: bool = Field(
        default=False,
        description="Ingest Dremio system tables in catalog"
    )

    match_fully_qualified_names: bool = Field(
        default=False,
        description="Whether `schema_pattern` is matched against fully qualified schema name `<database>.<schema>`.",
    )

    schema_pattern: Optional[AllowDenyPattern] = AllowDenyPattern.allow_all()

    data_product_pattern: dict = None
    data_product_specs: dict = None
    include_table_rename_lineage: bool = True
    include_copy_lineage: bool = True


