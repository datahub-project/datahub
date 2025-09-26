"""
Shared configuration mixin for stored procedure functionality across SQL sources.

Follows DataHub's established ConfigMixin pattern for shared configuration concerns.
"""

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel


class StoredProcedureConfigMixin(ConfigModel):
    """
    Configuration mixin for stored procedure functionality.
    SQL sources can inherit from this mixin to get consistent stored procedure configuration.

    Following PostgreSQL's simple and reliable approach:
    - Always include procedure code (needed for lineage parsing)
    - Process lineage immediately rather than deferring
    """

    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures.",
    )

    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion. "
        "Specify regex to match the entire procedure name in the format expected by the specific SQL source. "
        "e.g., 'database.procedure_name' for two-tier systems like MySQL/MariaDB or 'database.schema.procedure_name' for three-tier systems.",
    )
