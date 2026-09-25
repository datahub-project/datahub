"""Convex metadata ingestion source."""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.convex.client import (
    ConvexStreamingExportClient,
    RowCount,
)
from datahub.ingestion.source.convex.config import (
    ConvexDeploymentConfig,
    ConvexSourceConfig,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    UnionTypeClass,
)
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)

PLATFORM_NAME = "convex"

# Bookkeeping fields that Convex adds to every streaming-export document. They are
# not part of the user-defined table schema, so they are dropped from the emitted
# schema to keep it faithful to what the application actually declares.
SYSTEM_FIELDS = {"_table", "_component", "_ts", "_deleted"}


class ConvexDeploymentKey(ContainerKey):
    deployment: str


@dataclass
class ConvexSourceReport(SourceReport):
    deployments_scanned: int = 0
    tables_scanned: int = 0
    tables_filtered: LossyList[str] = field(default_factory=LossyList)
    row_counts_capped: LossyList[str] = field(default_factory=LossyList)


def map_json_schema_field(name: str, prop: Dict[str, Any]) -> SchemaFieldClass:
    """Map one JSON Schema property to a DataHub schema field."""
    json_type = prop.get("type")
    field_type: Any
    if "anyOf" in prop:
        variants = [variant.get("type", "object") for variant in prop["anyOf"]]
        field_type = UnionTypeClass()
        native = f"anyOf({', '.join(str(variant) for variant in variants)})"
    elif json_type == "string":
        field_type = StringTypeClass()
        native = "string"
    elif json_type in ("number", "integer"):
        field_type = NumberTypeClass()
        native = str(json_type)
    elif json_type == "boolean":
        field_type = BooleanTypeClass()
        native = "boolean"
    elif json_type == "object":
        field_type = RecordTypeClass()
        native = "object"
    elif json_type == "array":
        item_type = (prop.get("items") or {}).get("type", "any")
        field_type = ArrayTypeClass(nestedType=[str(item_type)])
        native = f"array<{item_type}>"
    else:
        field_type = NullTypeClass()
        native = str(json_type)

    return SchemaFieldClass(
        fieldPath=name,
        type=SchemaFieldDataTypeClass(type=field_type),
        nativeDataType=native,
        # Convex describes document references as `Id(<table>)` in this key.
        description=prop.get("$description"),
        nullable=False,  # Overwritten by the caller, which knows the required list.
    )


def schema_fields_from_json_schema(
    table_schema: Dict[str, Any],
) -> List[SchemaFieldClass]:
    """Map a Convex table's JSON Schema to DataHub schema fields."""
    properties: Dict[str, Any] = table_schema.get("properties", {})
    required = set(table_schema.get("required", []))
    fields = []
    for name, prop in properties.items():
        if name in SYSTEM_FIELDS:
            continue
        schema_field = map_json_schema_field(name, prop)
        schema_field.nullable = name not in required
        fields.append(schema_field)
    return fields


@platform_name("Convex")
@config_class(ConvexSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.CONTAINERS, "One container per Convex deployment")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DESCRIPTIONS,
    "Document reference fields carry their `Id(<table>)` description",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Row counts only, via `include_row_counts` (enabled by default)",
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Each deployment is its own container, so platform instances are not used",
    supported=False,
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Convex does not expose cross-table lineage",
    supported=False,
)
class ConvexSource(Source):
    """
    Ingests table metadata from one or more [Convex](https://convex.dev) deployments.

    Convex exposes a [streaming export API](https://docs.convex.dev/http-api/#streaming-export)
    on every deployment, which this source uses to discover each table, its JSON
    Schema, and (optionally) its row count. No Convex-side setup is needed beyond a
    deploy key with read access.

    Each deployment becomes a container, and each table in it becomes a dataset whose
    schema is mapped from Convex's JSON Schema — including unions (`anyOf`), nested
    objects, and arrays.
    """

    report: ConvexSourceReport

    def __init__(self, config: ConvexSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = ConvexSourceReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "ConvexSource":
        return cls(ConvexSourceConfig.parse_obj(config_dict), ctx)

    def get_report(self) -> ConvexSourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for deployment in self.config.deployments:
            yield from self._ingest_deployment(deployment)

    def _ingest_deployment(
        self, deployment: ConvexDeploymentConfig
    ) -> Iterable[MetadataWorkUnit]:
        client = ConvexStreamingExportClient(
            deployment.url, deployment.deploy_key.get_secret_value()
        )
        try:
            schemas = client.list_schemas()
        except Exception as e:
            self.report.failure(
                title="Cannot reach deployment",
                message="Failed to list table schemas for a Convex deployment",
                context=f"{deployment.name} ({deployment.url})",
                exc=e,
            )
            return

        self.report.deployments_scanned += 1
        logger.info(f"Discovered {len(schemas)} tables in {deployment.name}")

        container_key = ConvexDeploymentKey(
            platform=PLATFORM_NAME,
            deployment=deployment.name,
            env=self.config.env,
        )
        yield from gen_containers(
            container_key=container_key,
            name=deployment.name,
            sub_types=[DatasetContainerSubTypes.CONVEX_DEPLOYMENT],
            description=f"Convex deployment `{deployment.name}` at {deployment.url}",
        )

        for table, table_schema in sorted(schemas.items()):
            if not self.config.table_pattern.allowed(f"{deployment.name}.{table}"):
                self.report.tables_filtered.append(f"{deployment.name}.{table}")
                continue
            self.report.tables_scanned += 1
            yield from self._ingest_table(
                deployment, client, container_key, table, table_schema
            )

    def _ingest_table(
        self,
        deployment: ConvexDeploymentConfig,
        client: ConvexStreamingExportClient,
        container_key: ConvexDeploymentKey,
        table: str,
        table_schema: Dict[str, Any],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = make_dataset_urn(
            platform=PLATFORM_NAME,
            name=f"{deployment.name}.{table}",
            env=self.config.env,
        )

        row_count = self._count_rows(client, deployment.name, table)

        custom_properties = {"deployment_url": deployment.url, "table": table}
        if row_count is not None:
            custom_properties["row_count"] = (
                str(row_count.count) if row_count.exact else f"{row_count.count}+"
            )

        aspects: List[Any] = [
            StatusClass(removed=False),
            DatasetPropertiesClass(
                name=table,
                description=f"Convex table `{table}` in deployment `{deployment.name}`.",
                customProperties=custom_properties,
            ),
            SubTypesClass(typeNames=[DatasetSubTypes.TABLE]),
            SchemaMetadataClass(
                schemaName=f"{deployment.name}.{table}",
                platform=make_data_platform_urn(PLATFORM_NAME),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(
                    rawSchema=json.dumps(table_schema, indent=2)
                ),
                fields=schema_fields_from_json_schema(table_schema),
            ),
        ]
        if row_count is not None:
            aspects.append(
                DatasetProfileClass(
                    timestampMillis=int(time.time() * 1000),
                    rowCount=row_count.count,
                    columnCount=len(
                        [
                            name
                            for name in table_schema.get("properties", {})
                            if name not in SYSTEM_FIELDS
                        ]
                    ),
                )
            )

        for aspect in aspects:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=aspect
            ).as_workunit()

        yield from add_dataset_to_container(
            container_key=container_key, dataset_urn=dataset_urn
        )

    def _count_rows(
        self, client: ConvexStreamingExportClient, deployment_name: str, table: str
    ) -> Optional[RowCount]:
        if not self.config.include_row_counts:
            return None
        try:
            row_count = client.count_rows(table, self.config.max_count_pages)
        except Exception as e:
            self.report.warning(
                title="Row count failed",
                message="Failed to count the rows of a Convex table",
                context=f"{deployment_name}.{table}",
                exc=e,
            )
            return None
        if not row_count.exact:
            self.report.row_counts_capped.append(f"{deployment_name}.{table}")
        return row_count
