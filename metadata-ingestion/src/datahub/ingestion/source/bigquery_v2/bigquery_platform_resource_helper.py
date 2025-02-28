import logging
from dataclasses import dataclass
from typing import Optional

import cachetools
from pydantic import BaseModel, ValidationError

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import TagUrn

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class BigQueryLabel:
    key: str
    value: Optional[str]

    def primary_key(self) -> str:
        return f"{self.key}/{self.value}" if self.value else f"{self.key}"


class BigQueryLabelInfo(BaseModel):
    datahub_urn: str
    managed_by_datahub: bool
    key: str
    value: str


@dataclass()
class BigQueryLabelPlatformResource:
    datahub_urn: str
    project: Optional[str]
    managed_by_datahub: bool
    label: BigQueryLabel

    def platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="bigquery",
            resource_type="BigQueryLabelInfo",
            platform_instance=None,
            primary_key=self.label.primary_key(),
        )

    def platform_resource_info(self) -> BigQueryLabelInfo:
        bq_label_info = BigQueryLabelInfo(
            datahub_urn=self.datahub_urn,
            managed_by_datahub=self.managed_by_datahub,
            key=self.label.key,
            value=self.label.value,
        )
        return bq_label_info

    def platform_resource(self) -> PlatformResource:
        return PlatformResource.create(
            key=self.platform_resource_key(),
            secondary_keys=[self.datahub_urn],
            value=self.platform_resource_info(),
        )


class BigQueryPlatformResourceHelper:
    def __init__(
        self,
        bq_project: Optional[str],
        graph: Optional[DataHubGraph],
    ):
        self.bq_project = bq_project
        self.graph = graph

    platform_resource_cache: cachetools.LRUCache = cachetools.LRUCache(maxsize=500)

    def get_platform_resource(
        self, platform_resource_key: PlatformResourceKey
    ) -> Optional[PlatformResource]:
        # if graph is not available we always create a new PlatformResource
        if not self.graph:
            return None
        if self.platform_resource_cache.get(platform_resource_key.primary_key):
            return self.platform_resource_cache.get(platform_resource_key.primary_key)

        platform_resource = PlatformResource.from_datahub(
            key=platform_resource_key, graph_client=self.graph
        )
        if platform_resource:
            self.platform_resource_cache[platform_resource_key.primary_key] = (
                platform_resource
            )
            return platform_resource
        return None

    def generate_label_platform_resource(
        self,
        bigquery_label: BigQueryLabel,
        tag_urn: TagUrn,
        managed_by_datahub: bool = True,
    ) -> PlatformResource:
        new_platform_resource = BigQueryLabelPlatformResource(
            datahub_urn=tag_urn.urn(),
            project=self.bq_project,
            managed_by_datahub=managed_by_datahub,
            label=bigquery_label,
        )

        platform_resource = self.get_platform_resource(
            new_platform_resource.platform_resource_key()
        )
        if platform_resource:
            if (
                platform_resource.resource_info
                and platform_resource.resource_info.value
            ):
                try:
                    existing_info: Optional[BigQueryLabelInfo] = (
                        platform_resource.resource_info.value.as_pydantic_object(  # type: ignore
                            BigQueryLabelInfo
                        )
                    )
                except ValidationError as e:
                    logger.error(
                        f"Error converting existing value to BigQueryLabelInfo: {e}. Creating new one. Maybe this is because of a non backward compatible schema change."
                    )
                    existing_info = None

                if existing_info:
                    if (
                        new_platform_resource.platform_resource_info() == existing_info
                        or existing_info.managed_by_datahub
                    ):
                        return platform_resource
                    else:
                        raise ValueError(
                            f"Datahub URN mismatch for platform resources. Old (existing) platform resource: {platform_resource} and new platform resource: {new_platform_resource}"
                        )

        logger.info(f"Created platform resource {new_platform_resource}")

        self.platform_resource_cache.update(
            {
                new_platform_resource.platform_resource_key().primary_key: new_platform_resource.platform_resource()
            }
        )

        return new_platform_resource.platform_resource()
