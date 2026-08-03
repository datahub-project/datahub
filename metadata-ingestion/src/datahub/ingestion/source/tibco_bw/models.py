from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.source.tibco_bw.constants import (
    DEPLOYMENT_CLOUD,
    DEPLOYMENT_ON_PREM,
    DESTINATION_TYPE_QUEUE,
    DESTINATION_TYPE_TOPIC,
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


# --- Application archive (EAR) models ---------------------------------------
# What a BusinessWorks process declares about the messages it publishes and
# consumes, read from the deployed archive.


class JmsDestinationType(StrEnum):
    QUEUE = DESTINATION_TYPE_QUEUE
    TOPIC = DESTINATION_TYPE_TOPIC


class JmsMessageField(BaseModel):
    # `path` is dot-delimited for nested elements, matching how DataHub addresses
    # a nested schema field.
    path: str
    xsd_type: str
    nullable: bool = True


class JmsMessageSchema(BaseModel):
    """A message declared by one JMS activity, keyed by the destination it uses."""

    destination_name: str
    destination_type: JmsDestinationType
    fields: List[JmsMessageField] = Field(default_factory=list)
    # The XSD the element was declared in, kept verbatim so the contract can be
    # read in DataHub rather than only its flattened shape.
    raw_schema: str = ""
    # Where the declaration came from, for provenance on the emitted dataset.
    declared_by: str = ""
    archive: str = ""
    element_name: str = ""
    # A publisher's message becomes the destination's schema; a consumer's tells
    # us what it reads, which is lineage rather than a declaration.
    publishes: bool = True
