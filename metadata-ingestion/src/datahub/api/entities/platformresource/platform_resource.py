import logging
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Type, Union, cast

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
from datahub.metadata.urns import (
    DataPlatformInstanceUrn,
    DataPlatformUrn,
    PlatformResourceUrn,
    Urn,
)
from datahub.utilities.openapi_utils import OpenAPIGraphClient
from datahub.utilities.search_utils import (
    ElasticDocumentQuery,
    ElasticsearchQueryBuilder,
    LogicalOperator,
    SearchField,
)

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


class UrnSearchField(SearchField):
    """
    A search field that supports URN values.
    TODO: Move this to search_utils after we make this more generic.
    """

    def __init__(self, field_name: str, urn_value_extractor: Callable[[str], Urn]):
        self.urn_value_extractor = urn_value_extractor
        super().__init__(field_name)

    def get_search_value(self, value: str) -> str:
        return str(self.urn_value_extractor(value))


class PlatformResourceSearchField(SearchField):
    def __init__(self, field_name: str):
        super().__init__(field_name)

    @classmethod
    def from_search_field(
        cls, search_field: SearchField
    ) -> "PlatformResourceSearchField":
        # pretends to be a class method, but just returns the input
        return search_field  # type: ignore


class PlatformResourceSearchFields:
    PRIMARY_KEY = PlatformResourceSearchField("primaryKey")
    RESOURCE_TYPE = PlatformResourceSearchField("resourceType")
    SECONDARY_KEYS = PlatformResourceSearchField("secondaryKeys")
    PLATFORM = PlatformResourceSearchField.from_search_field(
        UrnSearchField(
            field_name="platform.keyword",
            urn_value_extractor=DataPlatformUrn.create_from_id,
        )
    )
    PLATFORM_INSTANCE = PlatformResourceSearchField.from_search_field(
        UrnSearchField(
            field_name="platformInstance.keyword",
            urn_value_extractor=DataPlatformInstanceUrn.from_string,
        )
    )


class ElasticPlatformResourceQuery(ElasticDocumentQuery[PlatformResourceSearchField]):
    def __init__(self):
        super().__init__()

    @classmethod
    def create_from(
        cls: Type["ElasticPlatformResourceQuery"],
        *args: Tuple[Union[str, PlatformResourceSearchField], str],
    ) -> "ElasticPlatformResourceQuery":
        return cast(ElasticPlatformResourceQuery, super().create_from(*args))


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
        """
        Creates a PlatformResource object with the removed status set to True.
        Removed PlatformResource objects are used to soft-delete resources from
        the graph.
        To hard-delete a resource, use the delete method.
        """
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
        """
        Fetches a PlatformResource from the graph given a key.
        Key can be either a PlatformResourceKey object or an urn string.
        Returns None if the resource is not found.
        """
        if isinstance(key, PlatformResourceKey):
            urn = PlatformResourceUrn(id=key.id)
        else:
            urn = PlatformResourceUrn.from_string(key)
        if not graph_client.exists(str(urn)):
            return None
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
        graph_client: DataHubGraph,
        key: str,
        primary: bool = True,
        is_exact: bool = True,
    ) -> Iterable["PlatformResource"]:
        """
        Searches for PlatformResource entities by primary or secondary key.

        :param graph_client: DataHubGraph client
        :param key: The key to search for
        :param primary: Whether to search for primary only or expand the search
            to secondary keys (default: True)
        :param is_exact: Whether to search for an exact match (default: True)
        :return: An iterable of PlatformResource objects
        """

        elastic_platform_resource_group = (
            ElasticPlatformResourceQuery.create_from()
            .group(LogicalOperator.OR)
            .add_field_match(
                PlatformResourceSearchFields.PRIMARY_KEY, key, is_exact=is_exact
            )
        )
        if not primary:  # we expand the search to secondary keys
            elastic_platform_resource_group.add_field_match(
                PlatformResourceSearchFields.SECONDARY_KEYS, key, is_exact=is_exact
            )
        query = elastic_platform_resource_group.end()
        openapi_client = OpenAPIGraphClient(graph_client)
        for urn in openapi_client.scroll_urns_by_filter(
            entity_type="platformResource",
            query=query,
        ):
            platform_resource = PlatformResource.from_datahub(graph_client, urn)
            if platform_resource:
                yield platform_resource

    def delete(self, graph_client: DataHubGraph, hard: bool = True) -> None:
        graph_client.delete_entity(str(PlatformResourceUrn(self.id)), hard=hard)

    @staticmethod
    def search_by_filters(
        graph_client: DataHubGraph,
        query: Union[
            ElasticPlatformResourceQuery,
            ElasticDocumentQuery,
            ElasticsearchQueryBuilder,
        ],
    ) -> Iterable["PlatformResource"]:
        openapi_client = OpenAPIGraphClient(graph_client)
        for urn in openapi_client.scroll_urns_by_filter(
            entity_type="platformResource",
            query=query,
        ):
            platform_resource = PlatformResource.from_datahub(graph_client, urn)
            if platform_resource:
                yield platform_resource
