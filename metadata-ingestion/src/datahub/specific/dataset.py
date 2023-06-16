from typing import Dict, Generic, List, Optional, TypeVar, Union
from urllib.parse import quote

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass as DatasetProperties,
    EditableDatasetPropertiesClass as EditableDatasetProperties,
    EditableSchemaMetadataClass as EditableSchemaMetadata,
    GlobalTagsClass as GlobalTags,
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass as GlossaryTerms,
    KafkaAuditHeaderClass,
    OwnerClass as Owner,
    OwnershipTypeClass,
    SchemaMetadataClass as SchemaMetadata,
    SystemMetadataClass,
    TagAssociationClass as Tag,
    UpstreamClass as Upstream,
    UpstreamLineageClass as UpstreamLineage,
)
from datahub.specific.custom_properties import CustomPropertiesPatchHelper
from datahub.specific.ownership import OwnershipPatchHelper
from datahub.utilities.urns.tag_urn import TagUrn
from datahub.utilities.urns.urn import Urn

T = TypeVar("T", bound=MetadataPatchProposal)


class FieldPatchHelper(Generic[T]):
    def __init__(
        self,
        parent: T,
        field_path: str,
        editable: bool = True,
    ) -> None:
        self._parent: T = parent
        self.field_path = field_path
        self.aspect_name = (
            EditableSchemaMetadata.ASPECT_NAME
            if editable
            else SchemaMetadata.ASPECT_NAME
        )
        self.aspect_field = "editableSchemaFieldInfo" if editable else "schemaFieldInfo"

    def add_tag(self, tag: Tag) -> "FieldPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path=f"/{self.aspect_field}/{self.field_path}/globalTags/tags/{tag.tag}",
            value=tag,
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "FieldPatchHelper":
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._parent._add_patch(
            self.aspect_name,
            "remove",
            path=f"/{self.aspect_field}/{self.field_path}/globalTags/tags/{tag}",
            value={},
        )
        return self

    def add_term(self, term: Term) -> "FieldPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path=f"/{self.aspect_field}/{self.field_path}/glossaryTerms/terms/{term.urn}",
            value=term,
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "FieldPatchHelper":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._parent._add_patch(
            self.aspect_name,
            "remove",
            path=f"/{self.aspect_field}/{self.field_path}/glossaryTerms/terms/{term}",
            value={},
        )
        return self

    def parent(self) -> T:
        return self._parent


class DatasetPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, "dataset", system_metadata=system_metadata, audit_header=audit_header
        )
        self.custom_properties_patch_helper = CustomPropertiesPatchHelper(
            self, DatasetProperties.ASPECT_NAME
        )
        self.ownership_patch_helper = OwnershipPatchHelper(self)

    def add_owner(self, owner: Owner) -> "DatasetPatchBuilder":
        self.ownership_patch_helper.add_owner(owner)
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "DatasetPatchBuilder":
        """
        param: owner_type is optional
        """
        self.ownership_patch_helper.remove_owner(owner, owner_type)
        return self

    def set_owners(self, owners: List[Owner]) -> "DatasetPatchBuilder":
        self.ownership_patch_helper.set_owners(owners)
        return self

    def add_upstream_lineage(self, upstream: Upstream) -> "DatasetPatchBuilder":
        self._add_patch(
            UpstreamLineage.ASPECT_NAME,
            "add",
            path=f"/upstreams/{quote(upstream.dataset, safe='')}",
            value=upstream,
        )
        return self

    def remove_upstream_lineage(
        self, dataset: Union[str, Urn]
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            UpstreamLineage.ASPECT_NAME,
            "remove",
            path=f"/upstreams/{dataset}",
            value={},
        )
        return self

    def set_upstream_lineages(self, upstreams: List[Upstream]) -> "DatasetPatchBuilder":
        self._add_patch(
            UpstreamLineage.ASPECT_NAME, "replace", path="/upstreams", value=upstreams
        )
        return self

    def add_tag(self, tag: Tag) -> "DatasetPatchBuilder":
        self._add_patch(
            GlobalTags.ASPECT_NAME, "add", path=f"/tags/{tag.tag}", value=tag
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "DatasetPatchBuilder":
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(GlobalTags.ASPECT_NAME, "remove", path=f"/tags/{tag}", value={})
        return self

    def add_term(self, term: Term) -> "DatasetPatchBuilder":
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "add", path=f"/terms/{term.urn}", value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "DatasetPatchBuilder":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "remove", path=f"/terms/{term}", value={}
        )
        return self

    def for_field(
        self, field_path: str, editable: bool = True
    ) -> FieldPatchHelper["DatasetPatchBuilder"]:
        """
        Get a helper that can perform patches against fields in the dataset

        :param field_path: The field path in datahub format
        :param editable: Whether patches should apply to the editable section of schema metadata or not
        """
        return FieldPatchHelper(
            self,
            field_path,
            editable=editable,
        )

    def set_description(
        self, description: str, editable: bool = False
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            DatasetProperties.ASPECT_NAME
            if not editable
            else EditableDatasetProperties.ASPECT_NAME,
            "replace",
            path="/description",
            value=description,
        )
        return self

    def set_custom_properties(
        self, custom_properties: Dict[str, str]
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            DatasetProperties.ASPECT_NAME,
            "replace",
            path="/customProperties",
            value=custom_properties,
        )
        return self

    def add_custom_property(self, key: str, value: str) -> "DatasetPatchBuilder":
        self.custom_properties_patch_helper.add_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> "DatasetPatchBuilder":
        self.custom_properties_patch_helper.remove_property(key)
        return self

    def set_display_name(self, display_name: str) -> "DatasetPatchBuilder":
        if display_name is not None:
            self._add_patch(
                DatasetProperties.ASPECT_NAME,
                "replace",
                path="/name",
                value=display_name,
            )
        return self
