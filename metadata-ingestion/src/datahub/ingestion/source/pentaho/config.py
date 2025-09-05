from typing import Dict, List, Optional

from pydantic import Field, validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    EnvConfigMixin,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class PentahoSourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, EnvConfigMixin
):
    # Required configuration
    kettle_file_paths: List[str] = Field(
        description="List of paths to Pentaho Kettle files (.ktr and .kjb files). Supports glob patterns."
    )
    
    # Optional configuration
    database_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from database connection names used in Pentaho to DataHub platform names. "
        "Example: {'mysql_conn': 'mysql', 'postgres_conn': 'postgres'}"
    )
    
    default_database_platform: str = Field(
        default="unknown",
        description="Default database platform to use when database connection is not found in database_mapping"
    )
    
    file_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Patterns to filter Kettle files by name"
    )
    
    include_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage information from TableInput and TableOutput steps"
    )
    
    include_job_metadata: bool = Field(
        default=True,
        description="Whether to extract DataJob metadata (name, type, description, custom properties)"
    )
    
    job_browse_path_template: str = Field(
        default="/pentaho/{job_type}/{file_name}",
        description="Template for generating browse paths for jobs. Available variables: {job_type}, {file_name}, {job_name}"
    )
    
    custom_properties_prefix: str = Field(
        default="pentaho.",
        description="Prefix to add to custom properties extracted from Pentaho files"
    )
    
    resolve_variables: bool = Field(
        default=False,
        description="Whether to attempt to resolve Pentaho variables in table names and SQL queries. "
        "If False, variables like ${tableString} will be preserved literally."
    )
    
    variable_values: Dict[str, str] = Field(
        default_factory=dict,
        description="Values for Pentaho variables to use when resolve_variables is True. "
        "Example: {'tableString': 'my_table', 'schema': 'public'}"
    )

    @validator("kettle_file_paths")
    def validate_file_paths(cls, v):
        if not v:
            raise ValueError("At least one kettle file path must be specified")
        return v

    @validator("database_mapping")
    def validate_database_mapping(cls, v):
        # Ensure all values are lowercase for consistency
        return {k: v.lower() for k, v in v.items()}