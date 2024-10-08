import logging
from typing import Dict, Iterable, List, Optional, Union

from avrogen.dict_wrapper import DictWrapper
from pydantic import BaseModel

import datahub.metadata.schema_classes as models
from datahub.api.entities.common.data_platform_instance import DataPlatformInstance
from datahub.api.entities.common.serialized_value import SerializedResourceValue
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import PlatformResourceUrn

logger = logging.getLogger(__name__)


class PlatformResourceKey(DatahubKey):
    platform: str
    platform_instance: Optional[str] = None
    resource_type: str
    primary_key: str

    @property
    def id(self) -> str:
        return self.guid()


class PlatformResourceInfo(BaseModel):
    resource_type: str
    primary_key: str
    value: Optional[SerializedResourceValue] = None
    secondary_keys: Optional[List[str]] = None

    @classmethod
    def from_resource_info(
        cls, resource_info: models.PlatformResourceInfoClass
    ) -> "PlatformResourceInfo":
        serialized_value: Optional[SerializedResourceValue] = None
        if resource_info.value:
            serialized_value = SerializedResourceValue.from_resource_value(
                resource_info.value
            )
        return cls(
            primary_key=resource_info.primaryKey,
            secondary_keys=resource_info.secondaryKeys,
            resource_type=resource_info.resourceType,
            value=serialized_value,
        )

    def to_resource_info(self) -> models.PlatformResourceInfoClass:
        value = None
        if self.value:
            value = models.SerializedValueClass(
                contentType=self.value.content_type,
                blob=self.value.blob,
                schemaType=self.value.schema_type,
                schemaRef=self.value.schema_ref,
            )
        return models.PlatformResourceInfoClass(
            primaryKey=self.primary_key,
            secondaryKeys=self.secondary_keys,
            resourceType=self.resource_type,
            value=value,
        )


class OpenAPIGraphClient:

    ENTITY_KEY_ASPECT_MAP = {
        aspect_type.ASPECT_INFO.get("keyForEntity"): name
        for name, aspect_type in models.ASPECT_NAME_MAP.items()
        if aspect_type.ASPECT_INFO.get("keyForEntity")
    }

    def __init__(self, graph: DataHubGraph):
        self.graph = graph
        self.openapi_base = graph._gms_server.rstrip("/") + "/openapi/v3"

    def scroll_urns_by_filter(
        self,
        entity_type: str,
        extra_or_filters: List[Dict[str, str]],
    ) -> Iterable[str]:
        """
        Scroll through all urns that match the given filters
        """

        key_aspect = self.ENTITY_KEY_ASPECT_MAP.get(entity_type)
        assert key_aspect, f"No key aspect found for entity type {entity_type}"

        count = 1000
        query = " OR ".join(
            [f"{filter['field']}:{filter['value']}" for filter in extra_or_filters]
        )
        scroll_id = None
        while True:
            response = self.graph._get_generic(
                self.openapi_base + f"/entity/{entity_type.lower()}",
                params={
                    "systemMetadata": "false",
                    "includeSoftDelete": "false",
                    "skipCache": "false",
                    "aspects": [key_aspect],
                    "scrollId": scroll_id,
                    "count": count,
                    "query": query,
                },
            )
            entities = response.get("entities", [])
            scroll_id = response.get("scrollId")
            for entity in entities:
                yield entity["urn"]
            if not scroll_id:
                break


class PlatformResource(BaseModel):
    id: str
    resource_info: Optional[PlatformResourceInfo] = None
    data_platform_instance: Optional[DataPlatformInstance] = None
    removed: bool = False

    @classmethod
    def remove(
        cls,
        key: PlatformResourceKey,
    ) -> "PlatformResource":
        return cls(
            id=key.id,
            removed=True,
        )

    @classmethod
    def create(
        cls,
        key: PlatformResourceKey,
        value: Union[Dict, DictWrapper, BaseModel],
        secondary_keys: Optional[List[str]] = None,
    ) -> "PlatformResource":
        return cls(
            id=key.id,
            resource_info=PlatformResourceInfo(
                resource_type=key.resource_type,
                primary_key=key.primary_key,
                secondary_keys=secondary_keys,
                value=SerializedResourceValue.create(value),
            ),
            removed=False,
            data_platform_instance=DataPlatformInstance(
                platform=make_data_platform_urn(key.platform),
                platform_instance=(
                    make_dataplatform_instance_urn(
                        platform=key.platform, instance=key.platform_instance
                    )
                    if key.platform_instance
                    else None
                ),
            ),
        )

    def to_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        dpi = (
            self.data_platform_instance.to_data_platform_instance()
            if self.data_platform_instance
            else None
        )
        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=str(PlatformResourceUrn(self.id)),
            aspects=[
                self.resource_info.to_resource_info() if self.resource_info else None,
                dpi,
                models.StatusClass(removed=self.removed),
            ],
        )

    def to_datahub(self, graph_client: DataHubGraph) -> None:
        for mcp in self.to_mcps():
            graph_client.emit(mcp)

    @classmethod
    def from_datahub(
        cls, graph_client: DataHubGraph, key: Union[PlatformResourceKey, str]
    ) -> Optional["PlatformResource"]:
        if isinstance(key, PlatformResourceKey):
            urn = PlatformResourceUrn(id=key.id)
        else:
            urn = PlatformResourceUrn.from_string(key)
        platform_resource = graph_client.get_entity_semityped(str(urn))
        return cls(
            id=urn.id,
            resource_info=(
                PlatformResourceInfo.from_resource_info(
                    platform_resource["platformResourceInfo"]
                )
                if "platformResourceInfo" in platform_resource
                else None
            ),
            data_platform_instance=(
                DataPlatformInstance.from_data_platform_instance(
                    platform_resource["dataPlatformInstance"]
                )
                if "dataPlatformInstance" in platform_resource
                else None
            ),
            removed=(
                platform_resource["status"].removed
                if "status" in platform_resource
                else False
            ),
        )

    @staticmethod
    def search_by_key(
        graph_client: DataHubGraph, key: str, primary: bool = True
    ) -> Iterable["PlatformResource"]:
        extra_or_filters = []
        extra_or_filters.append(
            {
                "field": "primaryKey",
                "condition": "EQUAL",
                "value": key,
            }
        )
        if not primary:  # we expand the search to secondary keys
            extra_or_filters.append(
                {
                    "field": "secondaryKeys",
                    "condition": "EQUAL",
                    "value": key,
                }
            )
        openapi_client = OpenAPIGraphClient(graph_client)
        for urn in openapi_client.scroll_urns_by_filter(
            entity_type="platformResource",
            extra_or_filters=extra_or_filters,
        ):
            platform_resource = PlatformResource.from_datahub(graph_client, urn)
            if platform_resource:
                yield platform_resource

    def delete(self, graph_client: DataHubGraph, hard: bool = True) -> None:
        graph_client.delete_entity(str(PlatformResourceUrn(self.id)), hard=hard)
