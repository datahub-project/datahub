import json
from abc import abstractmethod
from typing import Iterable, List, Optional, Union

import pandas as pd

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.ingestion.source.ib.ib_common import IBRedashSource, IBRedashSourceConfig
from datahub.ingestion.source.ib.utils.dataset_utils import (
    DatasetUtils,
    IBGenericPathElements,
    IBPathElementInfo,
    IBPathElementType,
)
from datahub.ingestion.source.state.stateful_ingestion_base import JobId
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BrowsePathsClass,
    BytesTypeClass,
    ChangeTypeClass,
    ContainerClass,
    DatasetPropertiesClass,
    DateTypeClass,
    KafkaSchemaClass,
    NullTypeClass,
    NumberTypeClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
)


class IBRedashDatasetSource(IBRedashSource):
    containers_cache = []

    @property
    @abstractmethod
    def platform(self):
        raise NotImplementedError("Sub-classes must define this variable.")

    @property
    @abstractmethod
    def object_subtype(self):
        raise NotImplementedError("Sub-classes must define this variable.")

    @abstractmethod
    def get_default_ingestion_job_id_prefix(self) -> JobId:
        pass

    def __init__(self, config: IBRedashSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: IBRedashSourceConfig = config

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        json_data = pd.read_json(json.dumps(self.query_get(self.config.query_id)))

        for i, row in json_data.iterrows():
            yield from self._fetch_object_workunits(row)

    def _fetch_object_workunits(self, row: pd.DataFrame) -> Iterable[MetadataWorkUnit]:
        object_name = row.objectName

        dataset_path = DatasetUtils.map_path(
            self.platform,
            self.object_subtype,
            IBGenericPathElements(
                location_code=row.locationCode,
                parent1=row.parent1,
                parent2=row.parent2,
                parent3=row.parent3,
                object_name=object_name,
            ),
        )

        properties = DatasetPropertiesClass(
            name=object_name,
            description=row.description,
            qualifiedName=IBRedashDatasetSource._build_dataset_qualified_name(
                *dataset_path
            ),
        )

        browse_paths = BrowsePathsClass(
            [f"/prod/{DatasetUtils.join_path('/', *dataset_path)}"]
        )

        columns = (
            list(
                map(
                    lambda col: IBRedashDatasetSource._map_column(col),
                    row.columns.split("|;|"),
                )
            )
            if pd.notna(row.columns)
            else []
        )
        schema = SchemaMetadataClass(
            schemaName=self.platform,
            version=1,
            hash="",
            platform=builder.make_data_platform_urn(self.platform),
            platformSchema=KafkaSchemaClass.construct_with_defaults(),
            fields=columns,
        )

        owners = []
        if row.owners is not None:
            owners = [
                builder.make_group_urn(owner.strip()) for owner in row.owners.split(",")
            ]

        ownership = builder.make_ownership_aspect_from_urn_list(
            owners, OwnershipSourceTypeClass.SERVICE, OwnershipTypeClass.TECHNICAL_OWNER
        )
        aspects = [properties, browse_paths, schema, ownership]
        snapshot = DatasetSnapshot(
            urn=DatasetUtils.build_dataset_urn(self.platform, *dataset_path),
            aspects=aspects,
        )
        mce = MetadataChangeEvent(proposedSnapshot=snapshot)
        yield MetadataWorkUnit(properties.qualifiedName, mce=mce)

        yield MetadataWorkUnit(
            id=f"{properties.qualifiedName}-subtype",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=snapshot.urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=[self.object_subtype]),
            ),
        )

        container_parent_path = None
        for i in range(1, len(dataset_path)):
            container_path = dataset_path[:i]
            if pd.isna(container_path[-1]):
                break
            yield from self._fetch_container_workunits(
                container_path, dataset_path[i - 1], container_parent_path
            )
            container_parent_path = container_path

        yield MetadataWorkUnit(
            id=f"{properties.qualifiedName}-container",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=snapshot.urn,
                aspectName="container",
                aspect=ContainerClass(
                    container=IBRedashDatasetSource._build_container_urn(
                        *container_parent_path
                    )
                ),
            ),
        )

    def _fetch_container_workunits(
        self,
        path: List[IBPathElementInfo],
        container_info: IBPathElementInfo,
        parent_path: Optional[List[IBPathElementInfo]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        qualified_name = IBRedashDatasetSource._build_dataset_qualified_name(*path)
        container_urn = builder.make_container_urn(qualified_name)
        if container_urn in self.containers_cache:
            return

        self.containers_cache.append(container_urn)

        yield IBRedashDatasetSource._build_container_workunit_with_aspect(
            container_urn,
            aspect=ContainerProperties(
                name=path[-1].value, qualifiedName=qualified_name
            ),
        )

        yield IBRedashDatasetSource._build_container_workunit_with_aspect(
            container_urn, SubTypesClass(typeNames=[container_info.name])
        )

        yield IBRedashDatasetSource._build_container_workunit_with_aspect(
            container_urn,
            aspect=DataPlatformInstance(
                platform=builder.make_data_platform_urn("infobip-location")
                if container_info.element_type == IBPathElementType.LOCATION
                else builder.make_data_platform_urn(self.platform),
            ),
        )

        if parent_path is not None:
            yield IBRedashDatasetSource._build_container_workunit_with_aspect(
                container_urn,
                ContainerClass(
                    container=IBRedashDatasetSource._build_container_urn(*parent_path)
                ),
            )

    @staticmethod
    def _build_container_workunit_with_aspect(urn: str, aspect):
        mcp = MetadataChangeProposalWrapper(
            entityType="container",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=f"{urn}",
            aspect=aspect,
        )
        return MetadataWorkUnit(id=f"{urn}-{type(aspect).__name__}", mcp=mcp)

    @staticmethod
    def _map_column(field) -> SchemaFieldClass:
        parts = field.split("|:|")
        data_type = parts[1]
        return SchemaFieldClass(
            fieldPath=parts[0],
            description=parts[3],
            type=SchemaFieldDataTypeClass(
                type=IBRedashDatasetSource._get_type_class(data_type)
            ),
            nativeDataType=data_type,
            nullable=bool(parts[2]),
        )

    @staticmethod
    def _build_container_urn(*path: str):
        return builder.make_container_urn(
            IBRedashDatasetSource._build_dataset_qualified_name(*path)
        )

    @staticmethod
    def _build_dataset_qualified_name(*path: IBPathElementInfo):
        return DatasetUtils.join_path(".", *path)

    @staticmethod
    def _get_type_class(type_str: str):
        type_str = type_str.lower() if type_str is not None else "undefined"
        type_class: Union[
            "StringTypeClass",
            "BooleanTypeClass",
            "NumberTypeClass",
            "BytesTypeClass",
            "DateTypeClass",
            "NullTypeClass",
        ]
        if type_str in [
            "string",
            "char",
            "nchar",
            "varchar",
            "varchar(n)",
            "varchar(max)",
            "nvarchar",
            "nvarchar(max)",
            "text",
        ]:
            return StringTypeClass()
        elif type_str in ["bit", "boolean"]:
            return BooleanTypeClass()
        elif type_str in [
            "integer",
            "int",
            "tinyint",
            "smallint",
            "bigint",
            "float",
            "real",
            "decimal",
            "numeric",
            "money",
        ]:
            return NumberTypeClass()
        elif type_str in ["object", "binary", "varbinary", "varbinary(max)"]:
            return BytesTypeClass()
        elif type_str in [
            "date",
            "smalldatetime",
            "datetime",
            "datetime2",
            "timestamp",
        ]:
            return DateTypeClass()
        elif type_str in ["array"]:
            return ArrayTypeClass()
        else:
            return NullTypeClass()
