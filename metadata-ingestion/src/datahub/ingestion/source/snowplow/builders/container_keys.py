"""
Container key definitions for Snowplow entities.

These keys are used with DataHub's gen_containers() for creating container entities.
"""

from pydantic import Field

from datahub.emitter.mcp_builder import ContainerKey


class SnowplowOrganizationKey(ContainerKey):
    """Container key for Snowplow BDP organizations."""

    organization_id: str = Field(description="Snowplow organization ID")


class SnowplowTrackingPlanKey(SnowplowOrganizationKey):
    """Container key for tracking plans within an organization."""

    plan_id: str = Field(description="Tracking plan ID")
