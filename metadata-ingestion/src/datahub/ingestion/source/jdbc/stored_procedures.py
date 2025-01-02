import logging
import traceback
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, Union

import jaydebeapi

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.jdbc.constants import ProcedureType
from datahub.ingestion.source.jdbc.containers import JDBCContainerKey
from datahub.ingestion.source.jdbc.reporting import JDBCSourceReport
from datahub.ingestion.source.jdbc.types import JDBCColumn, StoredProcedure
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    ContainerClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)


@dataclass
class StoredProcedureParameter(JDBCColumn):
    """Represents a stored procedure parameter."""

    mode: str  # IN, OUT, INOUT, RETURN

    def to_schema_field(self) -> SchemaFieldClass:
        base_field = super().to_schema_field()
        base_field.description = (
            f"{self.mode} parameter. {base_field.description or ''}"
        )

        # Ensure fieldPath is never empty by using a default if name is empty
        if not base_field.fieldPath:
            base_field.fieldPath = "unnamed_parameter"

        return base_field


@dataclass
class StoredProcedures:
    """
    Extracts stored procedure metadata from JDBC databases.
    """

    platform: str
    platform_instance: Optional[str]
    env: str
    schema_pattern: AllowDenyPattern
    report: JDBCSourceReport

    def extract_procedures(
        self, metadata: jaydebeapi.Connection.cursor, database_name: Optional[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Extract stored procedure metadata."""
        try:
            # Check if database supports schemas
            has_schema_support = True
            try:
                with metadata.getSchemas() as rs:
                    rs.next()
            except Exception:
                has_schema_support = False
                logger.debug("Database doesn't support schema metadata")

            # Try different catalog/schema combinations
            attempts: List[Tuple[Union[str, None], Union[str, None], None]] = [
                (None, None, None),
            ]

            if database_name:
                attempts.append((database_name, None, None))

            if has_schema_support:
                attempts.append(
                    (None, database_name, None)
                )  # Try with database as schema

            for catalog, schema_pattern, proc_pattern in attempts:
                try:
                    with metadata.getProcedures(
                        catalog, schema_pattern, proc_pattern
                    ) as proc_rs:
                        while proc_rs.next():
                            try:
                                proc = self._extract_procedure_info(
                                    proc_rs=proc_rs,
                                    has_schema_support=has_schema_support,
                                )
                                if not proc or not proc.name:
                                    continue

                                if proc.schema and not self.schema_pattern.allowed(
                                    proc.schema
                                ):
                                    continue

                                yield from self._generate_procedure_metadata(
                                    proc=proc,
                                    database=database_name,
                                    metadata=metadata,
                                    has_schema_support=has_schema_support,
                                )

                            except Exception as exc:
                                self.report.report_failure(
                                    message="Failed to extract stored procedure",
                                    context=f"{proc.schema}.{proc.name}"
                                    if proc
                                    else "unknown",
                                    exc=exc,
                                )
                    break  # If we got here, we successfully got procedures
                except Exception as e:
                    logger.debug(f"Attempt to get procedures failed: {e}")
                    continue

        except Exception as e:
            self.report.report_failure(
                "stored-procedures", f"Failed to extract stored procedures: {str(e)}"
            )
            logger.error(f"Failed to extract stored procedures: {str(e)}")
            logger.debug(traceback.format_exc())

    def _extract_procedure_info(
        self,
        proc_rs: jaydebeapi.Connection.cursor,
        has_schema_support: bool,
    ) -> Optional[StoredProcedure]:
        """Extract stored procedure information from result set."""
        try:
            # Try named columns first
            try:
                name = proc_rs.getString("PROCEDURE_NAME")
                schema = proc_rs.getString("PROCEDURE_SCHEM")
                remarks = proc_rs.getString("REMARKS")
                proc_type = proc_rs.getShort("PROCEDURE_TYPE")
            except Exception:
                # Fall back to positional columns
                name = proc_rs.getString(3)
                schema = proc_rs.getString(2)
                remarks = proc_rs.getString(7)
                proc_type = proc_rs.getShort(8)

            # Handle schema appropriately based on database type
            effective_schema = schema if has_schema_support else None

            return StoredProcedure(
                name=name,
                schema=effective_schema,
                remarks=remarks,
                proc_type=proc_type,
            )
        except Exception as e:
            logger.debug(f"Failed to extract procedure info: {e}")
            return None

    def _generate_procedure_metadata(
        self,
        proc: StoredProcedure,
        database: Optional[str],
        metadata: jaydebeapi.Connection.cursor,
        has_schema_support: bool,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate metadata for a stored procedure as a dataset."""
        full_name = f"{proc.schema}.{proc.name}" if proc.schema else proc.name
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform, full_name, self.platform_instance, self.env
        )

        # Get procedure parameters
        params = self._get_procedure_parameters(metadata, proc, has_schema_support)

        # Create schema fields from parameters
        fields = [param.to_schema_field() for param in params]

        # Create schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=full_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            fields=fields,
            platformSchema=OtherSchemaClass(rawSchema=proc.remarks or ""),
        )

        qualified_name = (
            f"{database}.{full_name}" if database and has_schema_support else full_name
        )

        # Emit schema metadata
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()

        # Dataset properties
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetProperties(
                name=proc.name,
                description=proc.remarks,
                customProperties={
                    "procedure_type": ProcedureType.from_value(proc.proc_type).name,
                    "language": proc.language or "SQL",
                },
                qualifiedName=qualified_name,
            ),
        ).as_workunit()

        # Set subtype as Stored Procedure
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=["Stored Procedure"]),
        ).as_workunit()

        # Add container relationship
        container_key = JDBCContainerKey(
            platform=make_data_platform_urn(self.platform),
            instance=self.platform_instance,
            env=self.env,
            key=proc.schema if proc.schema else database,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=container_key.as_urn()),
        ).as_workunit()

    def _get_procedure_parameters(
        self,
        metadata: jaydebeapi.Connection.cursor,
        proc: StoredProcedure,
        has_schema_support: bool,
    ) -> List[JDBCColumn]:
        """Get parameters for a stored procedure."""
        params: List = []
        param_counts: Dict = {}

        try:
            attempts = [
                (None, None if has_schema_support else proc.name, None),
                (None, proc.schema, proc.name),
            ]

            for schema, name, pattern in attempts:
                try:
                    with metadata.getProcedureColumns(
                        None, schema, name, pattern
                    ) as rs:
                        while rs.next():
                            # Map JDBC parameter types to meaningful strings
                            param_type = rs.getInt("COLUMN_TYPE")

                            # Skip RETURN parameters (type 5)
                            if param_type == 5:
                                continue

                            mode = {
                                1: "IN",
                                2: "INOUT",
                                4: "OUT",
                            }.get(param_type, "UNKNOWN")

                            # Get base parameter name
                            param_name = (
                                rs.getString("COLUMN_NAME") or f"param_{param_type}"
                            )

                            # Handle duplicate names by adding a suffix
                            if param_name in param_counts:
                                param_counts[param_name] += 1
                                param_name = f"{param_name}_{param_counts[param_name]}"
                            else:
                                param_counts[param_name] = 0

                            param = StoredProcedureParameter(
                                name=param_name,
                                type_name=rs.getString("TYPE_NAME"),
                                nullable=rs.getBoolean("NULLABLE"),
                                remarks=rs.getString("REMARKS"),
                                column_size=rs.getInt("PRECISION"),
                                decimal_digits=rs.getInt("SCALE"),
                                mode=mode,
                            )
                            params.append(param)
                    if params:
                        break
                except Exception as e:
                    logger.debug(
                        f"Could not get parameters with schema={schema}, name={name}: {e}"
                    )
                    continue

        except Exception as e:
            logger.debug(f"Could not get parameters for procedure {proc.name}: {e}")

        return params
