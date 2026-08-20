import logging
from typing import Iterable

from datahub.emitter.mce_builder import datahub_guid, make_container_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.hightouch.config import HightouchSourceConfig
from datahub.ingestion.source.hightouch.constants import (
    CONTAINER_TYPE_MODELS,
    CONTAINER_TYPE_SYNCS,
    HIGHTOUCH_PLATFORM,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)


class HightouchContainerHandler:
    def __init__(self, config: HightouchSourceConfig):
        self.config = config
        self._containers_emitted: set[str] = set()

    def get_models_container_urn(self) -> str:
        guid_dict = {
            "platform": HIGHTOUCH_PLATFORM,
            "container_type": "models",
        }
        if self.config.platform_instance:
            guid_dict["instance"] = self.config.platform_instance

        guid = datahub_guid(guid_dict)
        return make_container_urn(guid=guid)

    def get_syncs_container_urn(self) -> str:
        guid_dict = {
            "platform": HIGHTOUCH_PLATFORM,
            "container_type": "syncs",
        }
        if self.config.platform_instance:
            guid_dict["instance"] = self.config.platform_instance

        guid = datahub_guid(guid_dict)
        return make_container_urn(guid=guid)

    def emit_models_container(self) -> Iterable[MetadataWorkUnit]:
        container_urn = self.get_models_container_urn()
        if container_urn in self._containers_emitted:
            return

        self._containers_emitted.add(container_urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=ContainerPropertiesClass(
                name=CONTAINER_TYPE_MODELS,
                description="All Hightouch Models - data transformations and table references that power syncs",
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=SubTypesClass(typeNames=[DatasetContainerSubTypes.FOLDER]),
        ).as_workunit()

    def emit_syncs_container(self) -> Iterable[MetadataWorkUnit]:
        container_urn = self.get_syncs_container_urn()
        if container_urn in self._containers_emitted:
            return

        self._containers_emitted.add(container_urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=ContainerPropertiesClass(
                name=CONTAINER_TYPE_SYNCS,
                description="All Hightouch Syncs - data pipelines that move data from models to destinations",
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=SubTypesClass(typeNames=[DatasetContainerSubTypes.FOLDER]),
        ).as_workunit()

    def add_model_to_container(
        self, model_urn: DatasetUrn
    ) -> Iterable[MetadataWorkUnit]:
        container_urn = self.get_models_container_urn()

        yield MetadataChangeProposalWrapper(
            entityUrn=str(model_urn),
            aspect=ContainerClass(container=container_urn),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=str(model_urn),
            aspect=BrowsePathsV2Class(
                path=[BrowsePathEntryClass(id=CONTAINER_TYPE_MODELS)]
            ),
        ).as_workunit()

    def add_dataflow_to_container(
        self, dataflow_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        container_urn = self.get_syncs_container_urn()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataflow_urn,
            aspect=ContainerClass(container=container_urn),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataflow_urn,
            aspect=BrowsePathsV2Class(
                path=[BrowsePathEntryClass(id=CONTAINER_TYPE_SYNCS)]
            ),
        ).as_workunit()

    def add_datajob_browse_path(
        self, datajob_urn: str, dataflow_urn: str, pipeline_name: str
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=CONTAINER_TYPE_SYNCS),
                    BrowsePathEntryClass(id=pipeline_name, urn=dataflow_urn),
                ]
            ),
        ).as_workunit()
