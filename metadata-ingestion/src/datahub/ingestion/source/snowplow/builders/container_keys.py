"""
Container key definitions for Snowplow entities.

These keys are used with DataHub's gen_containers() for creating container entities.
"""

from pydantic import Field

from datahub.emitter.mcp_builder import ContainerKey


class SnowplowOrganizationKey(ContainerKey):
    """Container key for Snowplow BDP organizations."""

    organization_id: str = Field(description="Snowplow organization ID")


class SnowplowTrackingScenarioKey(SnowplowOrganizationKey):
    """Container key for tracking scenarios within an organization."""

    scenario_id: str = Field(description="Tracking scenario ID")


class SnowplowDataProductKey(SnowplowOrganizationKey):
    """Container key for data products within an organization."""

    product_id: str = Field(description="Data product ID")
