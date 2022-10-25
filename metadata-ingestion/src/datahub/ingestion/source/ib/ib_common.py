import json
import logging
import math
import pickle
import re
import sys
import zlib
from abc import abstractmethod
from typing import Dict, Iterable, List, Optional, Set, Union, cast

import pandas as pd
from pydantic.fields import Field
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.redash_state import (
    RedashCheckpointsList,
    RedashCheckpointState,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    DataPlatformInstance,
    Status,
)
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
    StatusClass,
    StringTypeClass,
    SubTypesClass,
)
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class IBRedashSourceStatefulIngestionConfig(StatefulIngestionConfig):
    remove_stale_metadata: bool = True


class RedashSourceReport(StatefulIngestionReport):
    events_skipped: int = 0
    events_deleted: int = 0


class IBRedashSourceConfig(StatefulIngestionConfigBase):
    connect_uri: str = Field(
        default="http://localhost:5000", description="Redash base URL."
    )
    api_key: str = Field(default="REDASH_API_KEY", description="Redash user API key.")
    query_id: str = Field(
        default="QUERY_ID",
        description="Target redash query",
    )
    api_page_limit: int = Field(
        default=sys.maxsize,
        description="Limit on number of pages queried for ingesting dashboards and charts API "
        "during pagination. ",
    )
    stateful_ingestion: Optional[IBRedashSourceStatefulIngestionConfig] = None


class IBRedashSource(StatefulIngestionSourceBase):
    batch_size = 1000
    config: IBRedashSourceConfig
    client: Redash
    report: RedashSourceReport

    def __init__(self, config: IBRedashSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBRedashSourceConfig = config
        self.report: RedashSourceReport = RedashSourceReport()

        self.config.connect_uri = self.config.connect_uri.strip("/")
        self.client = Redash(self.config.connect_uri, self.config.api_key)
        self.client.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # Handling retry and backoff
        retries = 3

        backoff_factor = 10
        status_forcelist = (500, 503, 502, 504)
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.client.session.mount("http://", adapter)
        self.client.session.mount("https://", adapter)

        self.api_page_limit = self.config.api_page_limit or math.inf

    @classmethod
    def create(cls, config_dict, ctx):
        config = IBRedashSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def query_get(self, query_id) -> str:
        url = f"//api/queries/{query_id}/results"
        return self.client._post(url).json()["query_result"]["data"]["rows"]

    @abstractmethod
    def fetch_workunits(self) -> Iterable[WorkUnit]:
        raise NotImplementedError("Sub-classes must implement this method.")

    def get_workunits(self) -> Iterable[WorkUnit]:
        if not self.is_stateful_ingestion_configured():
            for wu in self.fetch_workunits():
                self.report.report_workunit(wu)
                yield wu
            return

        last_state = self._load_last_state()
        current_state = self._create_current_state()

        for wu in self.fetch_workunits():
            if type(wu) is not MetadataWorkUnit:
                yield wu
                continue

            if current_state is not None:
                if type(wu.metadata) is MetadataChangeEvent:
                    urn = wu.metadata.proposedSnapshot.urn
                    wu.metadata.proposedSnapshot.aspects.append(
                        StatusClass(removed=False)
                    )
                elif type(wu.metadata) is MetadataChangeProposalWrapper:
                    urn = wu.metadata.entityUrn
                else:
                    raise TypeError(f"Unknown metadata type {type(wu.metadata)}")

                IBRedashSource._state_add_workunit(current_state, urn, wu)

                # Emitting workuntis not presented in last state
                if last_state is None or not IBRedashSource._state_has_workunit(
                    last_state, urn, wu
                ):
                    self.report.report_workunit(wu)
                    yield wu
                    continue
                else:
                    self.report.events_skipped += 1
            else:
                self.report.report_workunit(wu)
                yield wu

        if (
            self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
            and last_state is not None
            and current_state is not None
        ):
            # Deleting workunits not presented in current state
            for urn_str in last_state.keys() - current_state.keys():
                urn = Urn.create_from_string(urn_str)
                mcp = MetadataChangeProposalWrapper(
                    entityType=urn.get_type(),
                    entityUrn=urn_str,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=Status(removed=True),
                )
                self.report.events_deleted += 1
                yield MetadataWorkUnit(id=f"soft-delete-{urn_str}", mcp=mcp)

        if (
            self.config.stateful_ingestion
            and not self.config.stateful_ingestion.ignore_new_state
            and current_state is not None
        ):
            self._save_current_state(current_state)

    def close(self):
        self.prepare_for_commit()
        self.client.session.close()

    def get_report(self) -> RedashSourceReport:
        return self.report

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
            job_id.startswith(self.get_default_ingestion_job_id_prefix())
            and self.is_stateful_ingestion_configured()
            and self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        assert self.ctx.pipeline_name is not None
        if job_id.startswith(self.get_default_ingestion_job_id_prefix()):
            if job_id == self._get_checkpoints_list_job_id():
                state = RedashCheckpointsList()
            else:
                state = RedashCheckpointState()

            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.config,
                state=state,
            )
        return None

    def get_platform_instance_id(self) -> str:
        assert self.config.platform_instance is not None
        return self.config.platform_instance

    def _get_checkpoints_list_job_id(self):
        return self.get_default_ingestion_job_id_prefix() + "checkpoints_list"

    def _get_checkpoint_job_id(self, checkpoint_id: int):
        return self.get_default_ingestion_job_id_prefix() + str(checkpoint_id)

    def _load_last_state(self) -> Dict[str, Set[int]]:
        last_checkpoints_list = self.get_last_checkpoint(
            self._get_checkpoints_list_job_id(), RedashCheckpointsList
        )
        last_checkpoints_list_state = (
            cast(RedashCheckpointsList, last_checkpoints_list.state)
            if last_checkpoints_list is not None
            else None
        )
        if last_checkpoints_list_state is None:
            return None

        state: Dict[str, Set[int]] = dict()
        for checkpoint_id in last_checkpoints_list_state.get_ids():
            checkpoint = self.get_last_checkpoint(
                self._get_checkpoint_job_id(checkpoint_id), RedashCheckpointState
            )
            checkpoint_state = cast(RedashCheckpointState, checkpoint.state)
            for urn, wu_list in checkpoint_state.get_entries().items():
                state[urn] = wu_list

        return state

    def _create_current_state(self) -> Dict[str, Set[int]]:
        if (
            self.config.stateful_ingestion
            and not self.config.stateful_ingestion.ignore_new_state
        ):
            return dict()
        return None

    def _save_current_state(self, state: Dict[str, Set[int]]):
        cur_checkpoint_list = self.get_current_checkpoint(
            self._get_checkpoints_list_job_id()
        )

        if cur_checkpoint_list is None:
            return

        cur_checkpoint_list_state = cast(
            RedashCheckpointsList, cur_checkpoint_list.state
        )

        checkpoint_id: int = 0
        cur_checkpoint = self.get_current_checkpoint(
            self._get_checkpoint_job_id(checkpoint_id)
        )
        cur_checkpoint_state = cast(RedashCheckpointState, cur_checkpoint.state)
        cur_checkpoint_list_state.add_id(checkpoint_id)

        for urn, wu_list in state.items():
            cur_checkpoint_state.add_entry(urn, wu_list)
            if cur_checkpoint_state.size() >= 15000:
                checkpoint_id += 1
                cur_checkpoint = self.get_current_checkpoint(
                    self._get_checkpoint_job_id(checkpoint_id)
                )
                cur_checkpoint_state = cast(RedashCheckpointState, cur_checkpoint.state)
                cur_checkpoint_list_state.add_id(checkpoint_id)

    @abstractmethod
    def get_default_ingestion_job_id_prefix(self) -> JobId:
        raise NotImplementedError("Sub-classes must implement this method.")

    @staticmethod
    def _state_has_workunit(state: Dict[str, Set[int]], urn: str, wu: WorkUnit) -> bool:
        workunit_hash = IBRedashSource._hash_workunit(wu)
        return workunit_hash in state.get(urn, set())

    @staticmethod
    def _state_add_workunit(state: Dict[str, Set[int]], urn: str, wu: WorkUnit):
        workunits = state.get(urn, set())
        workunits.add(IBRedashSource._hash_workunit(wu))
        state[urn] = workunits

    @staticmethod
    def _hash_workunit(wu: WorkUnit):
        return zlib.crc32(pickle.dumps(wu)) & 0xFFFFFFFF


class IBPathElementInfo:
    subtype: str
    is_location: bool

    def __init__(self, subtype: str, is_location: bool = False):
        self.subtype: str = subtype
        self.is_location: bool = is_location


class IBRedashDatasetSource(IBRedashSource):
    containers_cache = []

    @property
    @abstractmethod
    def platform(self):
        raise NotImplementedError("Sub-classes must define this variable.")

    @property
    @abstractmethod
    def path_info(self) -> List[IBPathElementInfo]:
        raise NotImplementedError("Sub-classes must define this variable.")

    def __init__(self, config: IBRedashSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: IBRedashSourceConfig = config

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        json_data = pd.read_json(json.dumps(self.query_get(self.config.query_id)))

        for i, row in json_data.iterrows():
            yield from self.fetch_object_workunits(row)

    def fetch_object_workunits(self, row: pd.DataFrame) -> Iterable[MetadataWorkUnit]:
        object_name = row.objectName
        location_code = row.locationCode
        if pd.isna(object_name) or pd.isna(location_code):
            self.report.report_failure(
                "missing_mandatory_field",
                f"object_name is None or location_code is None in {row.to_string()}",
            )
            return

        dataset_path = self.normalize_dataset_path(
            [
                location_code,
                row.parent1,
                row.parent2,
                row.parent3,
                object_name,
            ]
        )

        properties = DatasetPropertiesClass(
            name=object_name,
            description=row.description,
            qualifiedName=build_dataset_qualified_name(*dataset_path),
        )

        browse_paths = BrowsePathsClass([build_dataset_browse_path(*dataset_path)])

        columns = (
            list(map(lambda col: self.map_column(col), row.columns.split("|;|")))
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
            urn=build_dataset_urn(self.platform, *dataset_path),
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
                aspect=SubTypesClass(typeNames=[self.path_info[-1].subtype]),
            ),
        )

        container_parent_path = None
        for i in range(1, len(dataset_path)):
            container_path = dataset_path[:i]
            if pd.isna(container_path[-1]):
                break
            yield from self.fetch_container_workunits(
                container_path, self.path_info[i - 1], container_parent_path
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
                    container=build_container_urn(*container_parent_path)
                ),
            ),
        )

    def fetch_container_workunits(
        self,
        path: List[str],
        container_info: IBPathElementInfo,
        parent_path: Optional[List[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        qualified_name = build_dataset_qualified_name(*path)
        container_urn = builder.make_container_urn(qualified_name)
        if container_urn in self.containers_cache:
            return

        self.containers_cache.append(container_urn)

        yield self.build_container_workunit_with_aspect(
            container_urn,
            aspect=ContainerProperties(name=path[-1], qualifiedName=qualified_name),
        )

        yield self.build_container_workunit_with_aspect(
            container_urn, SubTypesClass(typeNames=[container_info.subtype])
        )

        yield self.build_container_workunit_with_aspect(
            container_urn,
            aspect=DataPlatformInstance(
                platform=builder.make_data_platform_urn("infobip-location")
                if container_info.is_location
                else builder.make_data_platform_urn(self.platform),
            ),
        )

        if parent_path is not None:
            yield self.build_container_workunit_with_aspect(
                container_urn,
                ContainerClass(container=build_container_urn(*parent_path)),
            )

    @staticmethod
    def build_container_workunit_with_aspect(urn: str, aspect):
        mcp = MetadataChangeProposalWrapper(
            entityType="container",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=f"{urn}",
            aspect=aspect,
        )
        return MetadataWorkUnit(id=f"{urn}-{type(aspect).__name__}", mcp=mcp)

    @staticmethod
    def map_column(field) -> SchemaFieldClass:
        parts = field.split("|:|")
        data_type = parts[1]
        return SchemaFieldClass(
            fieldPath=parts[0],
            description=parts[3],
            type=SchemaFieldDataTypeClass(type=get_type_class(data_type)),
            nativeDataType=data_type,
            nullable=bool(parts[2]),
        )

    def normalize_dataset_path(self, dataset_path: List[str]):
        return list(
            map(
                lambda i_el: i_el[1].lower()
                if self.path_info[i_el[0]].is_location
                else i_el[1],
                enumerate(filter(lambda e: not pd.isna(e), dataset_path)),
            )
        )

    @abstractmethod
    def get_default_ingestion_job_id_prefix(self) -> JobId:
        pass


def get_type_class(type_str: str):
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
    elif type_str in ["date", "smalldatetime", "datetime", "datetime2", "timestamp"]:
        return DateTypeClass()
    elif type_str in ["array"]:
        return ArrayTypeClass()
    else:
        return NullTypeClass()


def build_dataset_urn(platform: str, *path: str):
    return builder.make_dataset_urn(
        platform.lower(),
        build_dataset_path_with_separator(".", *path),
        "PROD",
    )


def build_container_urn(*path: str):
    return builder.make_container_urn(build_dataset_qualified_name(*path))


def build_dataset_qualified_name(*path: str):
    return build_dataset_path_with_separator(".", *path)


def build_dataset_browse_path(*path: str):
    return f"/prod/{build_dataset_path_with_separator('/', *path)}"


def build_dataset_path_with_separator(separator: str, *path: str):
    return build_str_path_with_separator(separator, *path)


def build_str_path_with_separator(separator: str, *path: str):
    replace_chars_regex = re.compile("[/\\\\&?*=]")
    return separator.join(map(lambda p: replace_chars_regex.sub("-", p), path))
