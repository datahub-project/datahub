from datetime import datetime
from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict

from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    datahub_guid,
    make_data_job_urn,
)
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.source.common.subtypes import JobContainerSubTypes
from datahub.ingestion.source.sql.stored_procedures.constants import (
    STORED_PROCEDURES_CONTAINER,
)


class BaseProcedure(BaseModel):
    """
    Base class for stored procedure/function metadata.

    Important: default_db and default_schema use a three-value logic:
    - None: Use fallback from database_key/schema_key
    - Empty string (""): Explicitly no database/schema in lineage URNs
    - Non-empty string: Use this specific value in lineage URNs

    This distinction is critical for two-tier vs three-tier SQL sources
    to ensure procedure lineage URNs match table/view URN formats.
    """

    # Reject unknown fields so a mistyped kwarg fails loudly instead of being dropped.
    # Frozen because a procedure's metadata is fixed once read from the catalogue —
    # nothing in ingestion mutates it after construction, and making that explicit
    # lets the value be safely shared/hashed.
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    procedure_definition: Optional[str]
    created: Optional[datetime]
    last_altered: Optional[datetime]
    comment: Optional[str]
    argument_signature: Optional[str]
    return_type: Optional[str]
    language: str
    extra_properties: Optional[Dict[str, str]]
    default_db: Optional[str] = None
    default_schema: Optional[str] = None
    subtype: str = JobContainerSubTypes.STORED_PROCEDURE

    def get_procedure_identifier(self) -> str:
        if self.argument_signature:
            argument_signature_hash = datahub_guid(
                dict(argument_signature=self.argument_signature)
            )
            return f"{self.name}_{argument_signature_hash}"

        return self.name

    def to_urn(self, database_key: DatabaseKey, schema_key: Optional[SchemaKey]) -> str:
        return make_data_job_urn(
            orchestrator=database_key.platform,
            flow_id=get_procedure_flow_name(database_key, schema_key),
            job_id=self.get_procedure_identifier(),
            cluster=database_key.env or DEFAULT_ENV,
            platform_instance=database_key.instance,
        )


def get_procedure_flow_name(
    database_key: DatabaseKey, schema_key: Optional[SchemaKey]
) -> str:
    """Build flow name from database, schema, and container suffix, omitting empty parts."""
    parts = []

    if schema_key:
        if schema_key.database:
            parts.append(schema_key.database)
        parts.append(schema_key.db_schema)
    elif database_key.database:
        parts.append(database_key.database)

    parts.append(STORED_PROCEDURES_CONTAINER)
    return ".".join(parts)


class ProcedureCall(BaseModel):
    """A reference to a stored procedure invoked from another procedure body."""

    model_config = ConfigDict(frozen=True)

    database: Optional[str]
    # Named ``db_schema`` because ``schema`` shadows a BaseModel attribute.
    db_schema: Optional[str]
    name: str
