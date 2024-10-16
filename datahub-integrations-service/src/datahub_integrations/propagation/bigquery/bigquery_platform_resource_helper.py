import logging
from dataclasses import dataclass
from typing import Optional

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_platform_resource_helper import (
    BigQueryPlatformResourceHelper,
)
from datahub.metadata.urns import GlossaryTermUrn
from google.cloud.datacatalog_v1.types.policytagmanager import PolicyTag
from pydantic import BaseModel, ValidationError

logger: logging.Logger = logging.getLogger(__name__)


class BigQueryPolicyTagInfo(BaseModel):
    datahub_urn: str
    managed_by_datahub: bool
    name: str
    display_name: Optional[str]
    description: Optional[str]


@dataclass()
class BigQueryPolicyTagPlatformResource:
    datahub_urn: str
    project: Optional[str]
    managed_by_datahub: bool
    policy_tag: PolicyTag

    def platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="bigquery",
            resource_type="BigQueryPolicyTagInfo",
            platform_instance=self.project,
            # primary_key=b64encode(self.policy_tag.name.encode("utf-8")).decode("utf-8"),
            primary_key=self.policy_tag.name,
        )

    def platform_resource_info(self) -> BigQueryPolicyTagInfo:
        policy_tag_info = BigQueryPolicyTagInfo(
            datahub_urn=self.datahub_urn,
            managed_by_datahub=self.managed_by_datahub,
            name=self.policy_tag.name,
            display_name=self.policy_tag.display_name,
            description=self.policy_tag.description,
        )
        return policy_tag_info

    def platform_resource(self) -> PlatformResource:
        return PlatformResource.create(
            key=self.platform_resource_key(),
            secondary_keys=[self.datahub_urn],
            value=self.platform_resource_info(),
        )


class ExtendedBigQueryPlatformResourceHelper(BigQueryPlatformResourceHelper):
    def __init__(
        self,
        bq_project: Optional[str],
        graph: Optional[DataHubGraph],
    ):
        super().__init__(bq_project, graph)

    def generate_policy_tag_platform_resource(
        self,
        policy_tag: PolicyTag,
        glossary_urn: GlossaryTermUrn,
        managed_by_datahub: bool = True,
    ) -> PlatformResource:
        new_platform_resource = BigQueryPolicyTagPlatformResource(
            datahub_urn=glossary_urn.urn(),
            project=self.bq_project,
            managed_by_datahub=managed_by_datahub,
            policy_tag=policy_tag,
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
                    existing_info: Optional[BigQueryPolicyTagInfo] = platform_resource.resource_info.value.as_pydantic_object(BigQueryPolicyTagInfo)  # type: ignore
                except ValidationError as e:
                    logger.error(
                        f"Error converting existing value to BigQueryLabelInfo: {e}. Creating new one. Maybe this is because of a non backward compatible schema change."
                    )
                    existing_info = None

                if existing_info:
                    if new_platform_resource.platform_resource_info() == existing_info:
                        return new_platform_resource.platform_resource()
                    else:
                        if (
                            existing_info.datahub_urn
                            != new_platform_resource.datahub_urn
                        ):
                            raise ValueError(
                                f"Datahub URN mismatch for platform resources. Old (existing) platform resource: {platform_resource} and new platform resource: {new_platform_resource.platform_resource()}"
                            )
                else:
                    logger.info(
                        f"Updating platform resource {platform_resource} with new value {new_platform_resource.platform_resource_info()}"
                    )

        logger.info(
            f"Created platform resource {new_platform_resource.platform_resource()}"
        )

        self.platform_resource_cache.update(
            {
                new_platform_resource.platform_resource_key().primary_key: new_platform_resource.platform_resource()
            }
        )

        return new_platform_resource.platform_resource()
