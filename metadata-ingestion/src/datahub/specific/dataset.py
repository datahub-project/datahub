from typing import List, Optional, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass as Term,
    KafkaAuditHeaderClass,
    OwnerClass as Owner,
    OwnershipTypeClass,
    SystemMetadataClass,
    TagAssociationClass as Tag,
    UpstreamClass as Upstream,
)
from datahub.utilities.urns.tag_urn import TagUrn
from datahub.utilities.urns.urn import Urn


class FieldPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        dataset_urn: str,
        field_path: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
        editable: bool = True,
    ) -> None:
        super().__init__(
            dataset_urn,
            "dataset",
            system_metadata=system_metadata,
            audit_header=audit_header,
        )
        self.field_path = field_path
        self.aspect_name = "editableSchemaMetadata" if editable else "schemaMetadata"
        self.aspect_field = "editableSchemaFieldInfo" if editable else "schemaFieldInfo"

    def add_tag(self, tag: Tag) -> "FieldPatchBuilder":
        self._add_patch(
            self.aspect_name,
            "add",
            path=f"/{self.aspect_field}/{self.field_path}/globalTags/tags/{tag.tag}",
            value=tag,
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "FieldPatchBuilder":
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(
            self.aspect_name,
            "remove",
            path=f"/{self.aspect_field}/{self.field_path}/globalTags/tags/{tag}",
            value={},
        )
        return self

    def add_term(self, term: Term) -> "FieldPatchBuilder":
        self._add_patch(
            self.aspect_name,
            "add",
            path=f"/{self.aspect_field}/{self.field_path}/glossaryTerms/terms/{term.urn}",
            value=term,
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "FieldPatchBuilder":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            self.aspect_name,
            "remove",
            path=f"/{self.aspect_field}/{self.field_path}/glossaryTerms/terms/{term}",
            value={},
        )
        return self


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

    def add_owner(self, owner: Owner) -> "DatasetPatchBuilder":
        self._add_patch(
            "ownership", "add", path=f"/owners/{owner.owner}/{owner.type}", value=owner
        )
        return self

    def remove_owner(
        self, owner: Urn, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "DatasetPatchBuilder":
        """
        param: owner_type is optional
        """
        self._add_patch(
            "ownership",
            "remove",
            path=f"/owners/{owner}" + (f"/{owner_type}" if owner_type else ""),
            value=owner,
        )
        return self

    def set_owners(self, owners: List[Owner]) -> "DatasetPatchBuilder":
        self._add_patch("ownership", "replace", path="/owners", value=owners)
        return self

    def add_upstream_lineage(self, upstream: Upstream) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage",
            "add",
            path=f"/upstreams/{upstream.dataset}",
            value=upstream,
        )
        return self

    def remove_upstream_lineage(
        self, dataset: Union[str, Urn]
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage",
            "remove",
            path=f"/upstreams/{dataset}",
            value={},
        )
        return self

    def set_upstream_lineages(self, upstreams: List[Upstream]) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage", "replace", path="/upstreams", value=upstreams
        )
        return self

    def add_tag(self, tag: Tag) -> "DatasetPatchBuilder":
        self._add_patch("globalTags", "add", path=f"/tags/{tag.tag}", value=tag)
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "DatasetPatchBuilder":
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch("globalTags", "remove", path=f"/tags/{tag}", value={})
        return self

    def add_term(self, term: Term) -> "DatasetPatchBuilder":
        self._add_patch("glossaryTerms", "add", path=f"/terms/{term.urn}", value=term)
        return self

    def remove_term(self, term: Union[str, Urn]) -> "DatasetPatchBuilder":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch("glossaryTerms", "remove", path=f"/terms/{term}", value={})
        return self

    def field_patch_builder(
        self, field_path: str, editable: bool = True
    ) -> "FieldPatchBuilder":
        """
        Get a builder that can perform patches against fields in the dataset

        :param field_path: The field path in datahub format
        :param editable: Whether patches should apply to the editable section of schema metadata or not
        """
        return FieldPatchBuilder(
            self.urn,
            field_path,
            system_metadata=self.system_metadata,
            audit_header=self.audit_header,
            editable=editable,
        )
