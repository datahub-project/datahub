from dataclasses import dataclass, field as dataclass_field
from typing import Dict, Iterable, List, Union

from simple_salesforce.exceptions import SalesforceAuthenticationFailed

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.salesforce import SalesforceApi
from datahub.ingestion.source.salesforce_data360.client import (
    SalesforceData360Client,
)
from datahub.ingestion.source.salesforce_data360.config import (
    SalesforceData360Config,
)
from datahub.ingestion.source.salesforce_data360.constants import (
    CARDINALITY_ONE_TO_MANY,
    DMO_CREATION_TYPE_CUSTOM,
    FIELD_TYPE_MAPPING,
    PLATFORM,
    RELATIONSHIP_STATUS_ACTIVE,
)
from datahub.ingestion.source.salesforce_data360.models import (
    DataModelObject,
    DmoField,
    DmoRelationship,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    ForeignKeyConstraintClass,
    NullTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)
from datahub.sdk.dataset import Dataset
from datahub.utilities.lossy_collections import LossyList

METADATA_EXTRACTION = "Metadata Extraction"


@dataclass
class SalesforceData360SourceReport(StaleEntityRemovalSourceReport):
    dmos_scanned: int = 0
    dmos_ingested: int = 0
    filtered: LossyList[str] = dataclass_field(default_factory=LossyList)
    foreign_keys_emitted: int = 0
    inactive_relationships_skipped: int = 0
    unknown_field_types: LossyList[str] = dataclass_field(default_factory=LossyList)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


@platform_name("Salesforce")
@config_class(SalesforceData360Config)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
class SalesforceData360Source(StatefulIngestionSourceBase):
    """Ingests Salesforce Data 360 (Data Cloud) Data Model Objects, their fields
    and DMO-to-DMO relationships through the Data 360 Connect REST API."""

    config: SalesforceData360Config
    report: SalesforceData360SourceReport

    def __init__(self, config: SalesforceData360Config, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = SalesforceData360SourceReport()

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SalesforceData360Source":
        config = SalesforceData360Config.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Dataset]]:
        try:
            sf = SalesforceApi.create_salesforce_client(self.config)
        except SalesforceAuthenticationFailed as e:
            self.report.failure(
                title="Salesforce login failed",
                message="Please verify your credentials.",
                exc=e,
            )
            return
        client = SalesforceData360Client(sf)

        with self.report.new_stage(METADATA_EXTRACTION):
            for dmo in client.list_dmos():
                self.report.dmos_scanned += 1
                if not self._is_in_scope(dmo):
                    self.report.report_dropped(dmo.name)
                    continue
                try:
                    relationships = list(
                        client.list_relationships(dmo.name, self.config.dataspace)
                    )
                except Exception as e:
                    relationships = []
                    self.report.warning(
                        title="Failed to read DMO relationships",
                        message="The DMO is ingested without foreign keys.",
                        context=dmo.name,
                        exc=e,
                    )
                yield self._build_dataset(dmo, relationships)
                self.report.dmos_ingested += 1

    def _is_in_scope(self, dmo: DataModelObject) -> bool:
        if not self.config.dmo_pattern.allowed(dmo.name):
            return False
        return (
            self.config.include_disabled_dmos
            or dmo.is_enabled is not False
            or dmo.creation_type == DMO_CREATION_TYPE_CUSTOM
        )

    def _dataset_urn(self, name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=PLATFORM,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _build_dataset(
        self, dmo: DataModelObject, relationships: List[DmoRelationship]
    ) -> Dataset:
        dataset_urn = self._dataset_urn(dmo.name)
        schema = SchemaMetadataClass(
            schemaName=dmo.name,
            platform=make_data_platform_urn(PLATFORM),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[self._build_field(dmo.name, f) for f in dmo.fields],
            primaryKeys=[f.name for f in dmo.fields if f.is_primary_key] or None,
            foreignKeys=self._build_foreign_keys(dmo.name, dataset_urn, relationships)
            or None,
        )
        custom_properties: Dict[str, str] = {
            key: str(value)
            for key, value in {
                "category": dmo.category,
                "creationType": dmo.creation_type,
                "isEnabled": None
                if dmo.is_enabled is None
                else str(dmo.is_enabled).lower(),
                "dataSpaceName": dmo.data_space_name,
            }.items()
            if value is not None
        }
        return Dataset(
            platform=PLATFORM,
            name=dmo.name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            subtype=DatasetSubTypes.SALESFORCE_DATA_MODEL_OBJECT,
            display_name=dmo.label or dmo.name,
            description=dmo.description,
            custom_properties=custom_properties,
            schema=schema,
        )

    def _build_field(self, dmo_name: str, dmo_field: DmoField) -> SchemaFieldClass:
        type_class = FIELD_TYPE_MAPPING.get(dmo_field.type or "")
        if type_class is None:
            self.report.unknown_field_types.append(
                f"{dmo_name}.{dmo_field.name}: {dmo_field.type}"
            )
        return SchemaFieldClass(
            fieldPath=dmo_field.name,
            type=SchemaFieldDataTypeClass(
                type=type_class() if type_class else NullTypeClass()
            ),
            nativeDataType=dmo_field.type or "",
            label=dmo_field.label,
            isPartOfKey=dmo_field.is_primary_key,
        )

    def _build_foreign_keys(
        self,
        dmo_name: str,
        dataset_urn: str,
        relationships: List[DmoRelationship],
    ) -> List[ForeignKeyConstraintClass]:
        foreign_keys: List[ForeignKeyConstraintClass] = []
        for rel in relationships:
            referencing_object, referencing_field = rel.source_object, rel.source_field
            referenced_object, referenced_field = rel.target_object, rel.target_field
            # In a one-to-many relationship the "many" side holds the reference.
            if rel.cardinality == CARDINALITY_ONE_TO_MANY:
                referencing_object, referenced_object = (
                    referenced_object,
                    referencing_object,
                )
                referencing_field, referenced_field = (
                    referenced_field,
                    referencing_field,
                )
            # The endpoint lists a relationship on both of its DMOs; only the
            # side that holds the referencing field owns the foreign key.
            if referencing_object.name != dmo_name:
                continue
            if rel.status and rel.status != RELATIONSHIP_STATUS_ACTIVE:
                self.report.inactive_relationships_skipped += 1
                continue
            foreign_urn = self._dataset_urn(referenced_object.name)
            foreign_keys.append(
                ForeignKeyConstraintClass(
                    name=rel.name or referencing_field.name,
                    foreignDataset=foreign_urn,
                    foreignFields=[
                        make_schema_field_urn(foreign_urn, referenced_field.name)
                    ],
                    sourceFields=[
                        make_schema_field_urn(dataset_urn, referencing_field.name)
                    ],
                )
            )
        self.report.foreign_keys_emitted += len(foreign_keys)
        return foreign_keys

    def get_report(self) -> SalesforceData360SourceReport:
        return self.report
