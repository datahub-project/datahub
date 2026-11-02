import time
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Union

from pydantic import field_validator, model_validator
from ruamel.yaml import YAML

import datahub.emitter.mce_builder as builder
from datahub.api.entities.dataproduct.dataproduct import (
    InstitutionMemory,
    InstitutionMemoryElement,
    Ownership,
)
from datahub.configuration.common import ConfigModel
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DomainPropertiesClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    KafkaAuditHeaderClass,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SystemMetadataClass,
)
from datahub.specific.domain import DomainPatchBuilder
from datahub.utilities.urns.urn import Urn


def _parse_domain_urn(value: str, field: str) -> str:
    try:
        urn = Urn.from_string(value)
    except Exception as e:
        # Re-raise as ValueError; pydantic only wraps
        # ValueError/TypeError/AssertionError into ValidationError.
        raise ValueError(f"{field} {value!r} is not a valid URN: {e}") from e
    if urn.entity_type != "domain":
        raise ValueError(f"{field} {value!r} is not a domain URN")
    return urn.urn()


def _resolve_parent_domain_name(name: str, graph: DataHubGraph) -> str:
    # Same order as DomainRegistry: an existing domain id first, then a
    # display-name lookup. Unlike DomainRegistry, ids shaped like UUIDs are
    # not passed through unresolved.
    candidate = f"urn:li:domain:{name}"
    if graph.get_aspect(candidate, DomainPropertiesClass):
        return candidate
    urn = graph.get_domain_urn_by_name(name)
    if urn is None:
        raise ValueError(f"Parent domain {name!r} was not found in DataHub.")
    return urn


class Domain(ConfigModel):
    """A DataHub Domain that can be round-tripped to and from YAML.

    ``parent_domain`` accepts a display name, a domain id or a
    fully-qualified URN. Names are resolved against DataHub in
    :meth:`from_yaml`, so the MCP stream always emits canonical URNs.
    """

    id: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    parent_domain: Optional[str] = None
    owners: Optional[List[Union[str, Ownership]]] = None
    institutional_memory: Optional[InstitutionMemory] = None
    _resolved_parent_domain_urn: Optional[str] = None
    _original_yaml_dict: Optional[dict] = None

    @field_validator("id", mode="after")
    @classmethod
    def id_must_be_valid(cls, v: str) -> str:
        if not v:
            raise ValueError("id cannot be empty")
        if v.startswith("urn:li:"):
            _parse_domain_urn(v, "id")
        return v

    @property
    def urn(self) -> str:
        if self.id.startswith("urn:li:domain:"):
            return self.id
        return f"urn:li:domain:{self.id}"

    @property
    def resolved_parent_domain_urn(self) -> Optional[str]:
        """``parent_domain`` as a URN, once it is one or ``from_yaml`` resolved it."""
        return self._resolved_parent_domain_urn

    @model_validator(mode="after")
    def _resolve_urn_parent_domain(self) -> "Domain":
        if self.parent_domain and self.parent_domain.startswith("urn:li:"):
            self._resolved_parent_domain_urn = _parse_domain_urn(
                self.parent_domain, "parent_domain"
            )
        return self

    def _mint_auditstamp(self, message: str) -> AuditStampClass:
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    def _mint_owner(self, owner: Union[str, Ownership]) -> OwnerClass:
        if isinstance(owner, str):
            return OwnerClass(
                owner=builder.make_user_urn(owner),
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        assert isinstance(owner, Ownership)
        ownership_type, ownership_type_urn = builder.validate_ownership_type(owner.type)
        return OwnerClass(
            owner=builder.make_user_urn(owner.id),
            type=ownership_type,
            typeUrn=ownership_type_urn,
        )

    def generate_mcp(
        self, upsert: bool
    ) -> Iterable[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        """Emit the MCP stream that materializes this Domain in DataHub.

        ``upsert=True`` routes core properties through
        :class:`DomainPatchBuilder` so server-side fields like
        ``created`` survive. ``upsert=False`` snapshots the full
        :class:`DomainPropertiesClass` aspect.
        """
        if self.parent_domain and self._resolved_parent_domain_urn is None:
            raise Exception(
                f"Unable to generate MCPs because parent domain "
                f"{self.parent_domain!r} was not resolved to a URN. "
                "Pass a DataHubGraph to from_yaml() or supply a "
                "fully-qualified urn:li:domain:... id."
            )

        if upsert:
            patch = DomainPatchBuilder(self.urn)
            # Only patch the name when the YAML sets one, so an upsert never
            # renames an existing Domain to its raw id.
            if self.display_name is not None:
                patch.set_name(self.display_name)
            if self.description is not None:
                patch.set_description(self.description)
            if self._resolved_parent_domain_urn is not None:
                patch.set_parent_domain(self._resolved_parent_domain_urn)
            yield from patch.build()
        else:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=DomainPropertiesClass(
                    name=self.display_name or self.id,
                    description=self.description,
                    parentDomain=self._resolved_parent_domain_urn,
                ),
            )

        if self.owners:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=OwnershipClass(
                    owners=[self._mint_owner(o) for o in self.owners]
                ),
            )

        if self.institutional_memory and self.institutional_memory.elements:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=InstitutionalMemoryClass(
                    elements=[
                        InstitutionalMemoryMetadataClass(
                            url=element.url,
                            description=element.description,
                            createStamp=self._mint_auditstamp("yaml"),
                        )
                        for element in self.institutional_memory.elements
                    ]
                ),
            )

    def emit(
        self,
        emitter: Emitter,
        upsert: bool,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        for mcp in self.generate_mcp(upsert=upsert):
            emitter.emit(mcp, callback)

    @classmethod
    def from_yaml(
        cls,
        file: Path,
        graph: Optional[DataHubGraph] = None,
    ) -> "Domain":
        """Load a Domain from a YAML file.

        ``graph`` is required when ``parent_domain`` is a name or id
        rather than a URN.
        """
        with open(file) as fp:
            yaml = YAML(typ="rt")
            orig_dictionary = yaml.load(fp)
            parsed_domain = Domain.parse_obj_allow_extras(orig_dictionary)

            if (
                parsed_domain.parent_domain
                and not parsed_domain.parent_domain.startswith("urn:li:")
            ):
                if graph is None:
                    raise ValueError(
                        f"Domain {parsed_domain.id!r} references parent domain "
                        f"{parsed_domain.parent_domain!r} by name, but no DataHubGraph "
                        "was provided to resolve it. Pass a graph or use a fully-"
                        "qualified urn:li:domain:... value."
                    )
                parsed_domain._resolved_parent_domain_urn = _resolve_parent_domain_name(
                    parsed_domain.parent_domain, graph
                )

            parsed_domain._original_yaml_dict = orig_dictionary
            return parsed_domain

    @classmethod
    def from_datahub(cls, graph: DataHubGraph, id: str) -> "Domain":
        """Materialize a :class:`Domain` from a live DataHub instance."""
        properties = graph.get_aspect(id, DomainPropertiesClass)
        if not properties:
            raise Exception(f"Domain {id} has no domainProperties aspect.")

        owners_aspect: Optional[OwnershipClass] = graph.get_aspect(id, OwnershipClass)
        yaml_owners: Optional[List[Union[str, Ownership]]] = None
        if owners_aspect:
            yaml_owners = []
            for o in owners_aspect.owners:
                if o.type == OwnershipTypeClass.TECHNICAL_OWNER:
                    yaml_owners.append(o.owner)
                elif o.type == OwnershipTypeClass.CUSTOM:
                    yaml_owners.append(Ownership(id=o.owner, type=str(o.typeUrn)))
                else:
                    yaml_owners.append(Ownership(id=o.owner, type=str(o.type)))

        memory_aspect: Optional[InstitutionalMemoryClass] = graph.get_aspect(
            id, InstitutionalMemoryClass
        )
        institutional_memory = (
            InstitutionMemory(
                elements=[
                    InstitutionMemoryElement(
                        url=element.url, description=element.description
                    )
                    for element in memory_aspect.elements
                ]
            )
            if memory_aspect and memory_aspect.elements
            else None
        )

        return Domain(
            id=id,
            display_name=properties.name,
            description=properties.description,
            parent_domain=properties.parentDomain,
            owners=yaml_owners,
            institutional_memory=institutional_memory,
        )

    def to_yaml(self, file: Path) -> None:
        with open(file, "w") as fp:
            yaml = YAML(typ="rt")
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.default_flow_style = False
            yaml.dump(self.model_dump(exclude_unset=True, exclude_none=True), fp)

    @staticmethod
    def get_patch_builder(
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> DomainPatchBuilder:
        return DomainPatchBuilder(
            urn=urn, system_metadata=system_metadata, audit_header=audit_header
        )
