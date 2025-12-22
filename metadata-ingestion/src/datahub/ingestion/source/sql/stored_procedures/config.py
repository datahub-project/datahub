from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel


class StoredProcedureConfigMixin(ConfigModel):
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

    include_stored_procedures_code: bool = Field(
        default=True,
        description="Include SQL code for stored procedures in metadata. "
        "Set to false to reduce metadata volume if procedure definitions are not needed.",
    )
