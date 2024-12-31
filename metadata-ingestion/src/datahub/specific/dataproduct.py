from typing import List, Optional, Tuple, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    DataProductAssociationClass as DataProductAssociation,
    DataProductPropertiesClass as DataProductProperties,
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass as GlossaryTerms,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.utilities.urns.urn import Urn


class DataProductPatchBuilder(
    HasOwnershipPatch, HasCustomPropertiesPatch, HasTagsPatch, MetadataPatchProposal
):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn,
            system_metadata=system_metadata,
            audit_header=audit_header,
        )

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return DataProductProperties.ASPECT_NAME, ("customProperties",)

    def add_term(self, term: Term) -> "DataProductPatchBuilder":
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "add", path=("terms", term.urn), value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "DataProductPatchBuilder":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "remove", path=("terms", term), value={}
        )
        return self

    def set_name(self, name: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "add",
            path=("name",),
            value=name,
        )
        return self

    def set_description(self, description: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "add",
            path=("description",),
            value=description,
        )
        return self

    def set_assets(
        self, assets: List[DataProductAssociation]
    ) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "add",
            path=("assets",),
            value=assets,
        )
        return self

    def add_asset(self, asset_urn: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "add",
            path=("assets", asset_urn),
            value=DataProductAssociation(destinationUrn=asset_urn),
        )
        return self

    def remove_asset(self, asset_urn: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "remove",
            path=("assets", asset_urn),
            value={},
        )
        return self

    def set_external_url(self, external_url: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "add",
            path=("externalUrl",),
            value=external_url,
        )
        return self
