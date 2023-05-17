from typing import Dict, List, Optional, TypeVar, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    DataProductAssociationClass as DataProductAssociation,
    DataProductPropertiesClass as DataProductProperties,
    GlobalTagsClass as GlobalTags,
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass as GlossaryTerms,
    KafkaAuditHeaderClass,
    OwnerClass as Owner,
    OwnershipTypeClass,
    SystemMetadataClass,
    TagAssociationClass as Tag,
)
from datahub.specific.custom_properties import CustomPropertiesPatchHelper
from datahub.specific.ownership import OwnershipPatchHelper
from datahub.utilities.urns.tag_urn import TagUrn
from datahub.utilities.urns.urn import Urn

T = TypeVar("T", bound=MetadataPatchProposal)


class DataProductPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn,
            "dataProduct",
            system_metadata=system_metadata,
            audit_header=audit_header,
        )
        self.custom_properties_patch_helper = CustomPropertiesPatchHelper(
            self, DataProductProperties.ASPECT_NAME
        )
        self.ownership_patch_helper = OwnershipPatchHelper(self)

    def add_owner(self, owner: Owner) -> "DataProductPatchBuilder":
        self.ownership_patch_helper.add_owner(owner)
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "DataProductPatchBuilder":
        """
        param: owner_type is optional
        """
        self.ownership_patch_helper.remove_owner(owner, owner_type)
        return self

    def set_owners(self, owners: List[Owner]) -> "DataProductPatchBuilder":
        self.ownership_patch_helper.set_owners(owners)
        return self

    def add_tag(self, tag: Tag) -> "DataProductPatchBuilder":
        self._add_patch(
            GlobalTags.ASPECT_NAME, "add", path=f"/tags/{tag.tag}", value=tag
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "DataProductPatchBuilder":
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(GlobalTags.ASPECT_NAME, "remove", path=f"/tags/{tag}", value={})
        return self

    def add_term(self, term: Term) -> "DataProductPatchBuilder":
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "add", path=f"/terms/{term.urn}", value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "DataProductPatchBuilder":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "remove", path=f"/terms/{term}", value={}
        )
        return self

    def set_name(self, name: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "replace",
            path="/name",
            value=name,
        )
        return self

    def set_description(self, description: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "replace",
            path="/description",
            value=description,
        )
        return self

    def set_custom_properties(
        self, custom_properties: Dict[str, str]
    ) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "replace",
            path="/customProperties",
            value=custom_properties,
        )
        return self

    def add_custom_property(self, key: str, value: str) -> "DataProductPatchBuilder":
        self.custom_properties_patch_helper.add_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> "DataProductPatchBuilder":
        self.custom_properties_patch_helper.remove_property(key)
        return self

    def set_assets(
        self, assets: List[DataProductAssociation]
    ) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "replace",
            path="/assets",
            value=assets,
        )
        return self

    def add_asset(self, asset_urn: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "add",
            path=f"/assets/{asset_urn}",
            value=DataProductAssociation(destinationUrn=asset_urn),
        )
        return self

    def remove_asset(self, asset_urn: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "remove",
            path=f"/assets/{asset_urn}",
            value={},
        )
        return self

    def set_external_url(self, external_url: str) -> "DataProductPatchBuilder":
        self._add_patch(
            DataProductProperties.ASPECT_NAME,
            "replace",
            path="/external_url",
            value=external_url,
        )
        return self
