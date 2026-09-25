from typing import Optional, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    MetadataPatchProposal,
    determine_array_primary_keys,
)
from datahub.metadata.schema_classes import (
    DomainAssociationClass as DomainAssociation,
    DomainsClass as Domains,
)
from datahub.metadata.urns import DomainUrn, Urn

DEFAULT_DOMAIN_ASSOCIATIONS_KEY_FIELDS = [
    f"attribution{UNIT_SEPARATOR}source",
    "domain",
]
_DOMAINS_APK = {"domainAssociations": DEFAULT_DOMAIN_ASSOCIATIONS_KEY_FIELDS}


class HasDomainsPatch(MetadataPatchProposal):
    def add_domain(self, domain: DomainAssociation) -> Self:
        """Adds a domain association to the entity.

        Uses compound-key semantics keyed by ``(attribution.source, domain)``.
        Unattributed domains use ``source=""``; attributed domains use the actual source.

        Args:
            domain: The DomainAssociation object to add.

        Returns:
            The patch builder instance.
        """
        source = (
            domain.attribution.source
            if domain.attribution and domain.attribution.source
            else ""
        )
        self._add_patch(
            Domains.ASPECT_NAME,
            "add",
            path=("domainAssociations", source, domain.domain),
            value=domain,
            array_primary_keys=_DOMAINS_APK,
        )
        return self

    def remove_domain(
        self,
        domain: Union[str, Urn],
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Removes a domain association from the entity.

        Args:
            domain: The domain to remove, specified as a string or Urn object.
            attribution_source: When set, only the entry for that specific source is
                removed. When omitted, all entries for the specified domain urn are removed.

        Returns:
            The patch builder instance.
        """
        if isinstance(domain, str) and not domain.startswith("urn:li:domain:"):
            domain = DomainUrn(domain)
        source = str(attribution_source) if attribution_source is not None else None
        path, array_primary_keys = determine_array_primary_keys(
            field_name="domainAssociations",
            default_key_fields=DEFAULT_DOMAIN_ASSOCIATIONS_KEY_FIELDS,
            path=[source, str(domain)],
        )
        self._add_patch(
            Domains.ASPECT_NAME,
            "remove",
            path=("domainAssociations", *path),
            value={},
            array_primary_keys=array_primary_keys,
        )

        return self
