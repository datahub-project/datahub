from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.sap_mdg.constants import (
    DRF_ABAP_TRUE,
    DRF_FIELD_ACTIVE,
    DRF_FIELD_BUSINESS_SYSTEM,
    DRF_FIELD_DATA_MODEL,
    DRF_FIELD_MODEL,
)


class ODataVersion(str, Enum):
    V2 = "2"
    V4 = "4"
    UNKNOWN = "unknown"


class _ODataBaseModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class ODataProperty(_ODataBaseModel):
    name: str
    type_name: str
    nullable: bool = True
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    label: Optional[str] = None
    quickinfo: Optional[str] = None

    @property
    def description(self) -> Optional[str]:
        return self.label or self.quickinfo


class ODataReferentialConstraint(_ODataBaseModel):
    # A property on the dependent (foreign-key-holding) entity paired with the
    # property it references on the principal (referenced) entity.
    dependent_property: str
    principal_property: str


class ODataNavigationProperty(_ODataBaseModel):
    name: str
    # Populated for OData V4, where the target type is declared inline. For V2 the
    # target is resolved later via ``relationship`` + ``to_role`` against an Association.
    target_type_fqn: Optional[str] = None
    relationship: Optional[str] = None
    from_role: Optional[str] = None
    to_role: Optional[str] = None
    referential_constraints: List[ODataReferentialConstraint] = Field(
        default_factory=list
    )


class ODataEntityType(_ODataBaseModel):
    namespace: str
    name: str
    key_property_names: List[str] = Field(default_factory=list)
    properties: List[ODataProperty] = Field(default_factory=list)
    navigation_properties: List[ODataNavigationProperty] = Field(default_factory=list)
    label: Optional[str] = None

    @property
    def fqn(self) -> str:
        return f"{self.namespace}.{self.name}"


class NavigationTarget(_ODataBaseModel):
    # Resolved endpoint of a navigation property: the target entity type and the
    # referential constraints tying the two entities' columns together.
    target_type_fqn: Optional[str] = None
    referential_constraints: List[ODataReferentialConstraint] = Field(
        default_factory=list
    )


class ODataAssociationEnd(_ODataBaseModel):
    role: Optional[str] = None
    type_fqn: str


class ODataAssociation(_ODataBaseModel):
    namespace: str
    name: str
    ends: List[ODataAssociationEnd] = Field(default_factory=list)
    referential_constraints: List[ODataReferentialConstraint] = Field(
        default_factory=list
    )

    @property
    def fqn(self) -> str:
        return f"{self.namespace}.{self.name}"


class ODataEntitySet(_ODataBaseModel):
    name: str
    entity_type_fqn: str
    container_namespace: str
    label: Optional[str] = None


class ODataMetadata(_ODataBaseModel):
    version: ODataVersion = ODataVersion.UNKNOWN
    entity_types: List[ODataEntityType] = Field(default_factory=list)
    entity_sets: List[ODataEntitySet] = Field(default_factory=list)
    associations: List[ODataAssociation] = Field(default_factory=list)

    def entity_types_by_fqn(self) -> Dict[str, ODataEntityType]:
        return {entity_type.fqn: entity_type for entity_type in self.entity_types}

    def associations_by_fqn(self) -> Dict[str, ODataAssociation]:
        return {association.fqn: association for association in self.associations}

    def entity_sets_by_type_fqn(self) -> Dict[str, ODataEntitySet]:
        # First set wins when a type is exposed through several sets; foreign-key
        # resolution needs a single deterministic target per entity type.
        mapping: Dict[str, ODataEntitySet] = {}
        for entity_set in self.entity_sets:
            mapping.setdefault(entity_set.entity_type_fqn, entity_set)
        return mapping


class DrfReplicationModelRow(_ODataBaseModel):
    # A row of DRFC_APPL: a replication model (`model`) and the MDG data model it
    # governs, plus the active flag ('X' when the model replicates).
    model: str = Field(alias=DRF_FIELD_MODEL)
    data_model: Optional[str] = Field(default=None, alias=DRF_FIELD_DATA_MODEL)
    active_flag: Optional[str] = Field(default=None, alias=DRF_FIELD_ACTIVE)

    @property
    def is_active(self) -> bool:
        return (self.active_flag or "").strip().upper() == DRF_ABAP_TRUE


class DrfSystemRow(_ODataBaseModel):
    # A row of DRFC_APPL_SYS: a replication model assigned to a target business system.
    model: str = Field(alias=DRF_FIELD_MODEL)
    business_system: str = Field(alias=DRF_FIELD_BUSINESS_SYSTEM)


class DrfDistribution(BaseModel):
    # Normalized DRF outcome: for each MDG data model (`USMD_MODEL`), the ordered,
    # de-duplicated list of target business systems it is replicated to.
    targets_by_data_model: Dict[str, List[str]] = Field(default_factory=dict)

    def targets_for(self, data_model: str) -> List[str]:
        return self.targets_by_data_model.get(data_model, [])


class SapMdgServiceKey(ContainerKey):
    # Identity of the container DataHub creates per OData service; the service id is
    # what makes each service's container urn unique within the platform/instance/env.
    service: str
