from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.source.tibco_bw.constants import (
    DEPLOYMENT_CLOUD,
    DEPLOYMENT_ON_PREM,
)
from datahub.utilities.str_enum import StrEnum


class TibcoDeployment(StrEnum):
    ON_PREM = DEPLOYMENT_ON_PREM
    CLOUD = DEPLOYMENT_CLOUD


# --- Raw payload models -----------------------------------------------------
# These mirror the shapes returned by the bwagent (on-prem) and TIBCO Cloud
# Integration (cloud) REST APIs. They stay permissive (extra fields ignored,
# population by field name or alias) so minor version differences in the
# upstream payloads do not break ingestion.


class _RawModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class BwDomain(_RawModel):
    name: str
    description: Optional[str] = None


class BwAppSpace(_RawModel):
    name: str
    description: Optional[str] = None
    status: Optional[str] = None


class BwAppNode(_RawModel):
    name: str
    status: Optional[str] = None


class BwApplication(_RawModel):
    name: str
    version: Optional[str] = None
    state: Optional[str] = None
    app_type: Optional[str] = Field(default=None, alias="appType")


class TciSubscription(_RawModel):
    # `subscriptionId` locates the tenant in later app queries; TCI also refers
    # to it as the subscription locator.
    subscription_id: str = Field(alias="subscriptionId")
    name: Optional[str] = None
    organization: Optional[str] = Field(default=None, alias="orgDisplayName")
    region: Optional[str] = None


class TciApp(_RawModel):
    name: str
    app_type: Optional[str] = Field(default=None, alias="type")
    state: Optional[str] = Field(default=None, alias="status")
    version: Optional[str] = None
    description: Optional[str] = None


# --- Normalized models ------------------------------------------------------
# Both runtimes are projected onto the same two-level shape: a scope (appspace
# on-prem, subscription on cloud) that contains deployed applications.


class TibcoApplication(BaseModel):
    name: str
    description: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)


class TibcoScope(BaseModel):
    # `id` is stable and used as the DataFlow flow id, so it must not change
    # between runs for the same appspace/subscription.
    id: str
    name: str
    description: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)
    applications: List[TibcoApplication] = Field(default_factory=list)
