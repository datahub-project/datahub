import logging
import math
import sys
from abc import abstractmethod
from typing import Iterable, Optional, Union, cast

from pydantic.fields import Field
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.redash_state import RedashCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    ChangeTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StatusClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


class IBRedashSourceStatefulIngestionConfig(StatefulIngestionConfig):
    remove_stale_metadata: bool = True


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


class RedashSourceReport(StatefulIngestionReport):
    workunits_deleted: int = 0


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
                self.report.workunits_produced += 1
                yield wu
            return

        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        cur_checkpoint_state = (
            cast(RedashCheckpointState, cur_checkpoint.state)
            if cur_checkpoint is not None
            else None
        )

        for wu in self.fetch_workunits():
            self.report.workunits_produced += 1
            if type(wu) is not MetadataWorkUnit:
                yield wu
                continue
            wu.metadata.proposedSnapshot.aspects.append(StatusClass(removed=False))
            if cur_checkpoint_state is not None:
                cur_checkpoint_state.add_urn(wu.metadata.proposedSnapshot.urn)
            yield wu

        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), RedashCheckpointState
        )
        last_checkpoint_state = (
            cast(RedashCheckpointState, last_checkpoint.state)
            if last_checkpoint is not None
            else None
        )
        if (
                self.source_config.stateful_ingestion
                and self.source_config.stateful_ingestion.remove_stale_metadata
                and last_checkpoint_state is not None
                and cur_checkpoint_state is not None
        ):
            logger.info("deleting")
            for urn in last_checkpoint_state.get_urns_not_in(cur_checkpoint_state):
                self.report.workunits_deleted += 1
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=Status(removed=True),
                )
                yield MetadataWorkUnit(id=f"soft-delete-{urn}", mcp=mcp)

    def close(self):
        self.prepare_for_commit()
        self.client.session.close()

    def get_report(self) -> RedashSourceReport:
        return self.report

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
                job_id == self.get_default_ingestion_job_id()
                and self.is_stateful_ingestion_configured()
                and self.source_config.stateful_ingestion
                and self.source_config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.source_config,
                state=RedashCheckpointState(),
            )
        return None

    def get_platform_instance_id(self) -> str:
        assert self.source_config.platform_instance is not None
        return self.source_config.platform_instance

    @abstractmethod
    def get_default_ingestion_job_id(self) -> JobId:
        raise NotImplementedError("Sub-classes must implement this method.")


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


def build_dataset_urn(platform: str, location_code: str, *path: str):
    return builder.make_dataset_urn(
        platform.lower(), build_dataset_path_with_separator('.', location_code, *path), "PROD"
    )


def build_dataset_qualified_name(location_code: str, *path: str):
    return build_dataset_path_with_separator('.', location_code, *path)


def build_dataset_browse_path(location_code: str, *path: str):
    return f"/prod/{build_dataset_path_with_separator('/', location_code, *path)}"


def build_dataset_path_with_separator(separator: str, location_code: str, *path: str):
    return f"{location_code.lower()}{separator}{separator.join(filter(lambda e: e is not None, path))}"
