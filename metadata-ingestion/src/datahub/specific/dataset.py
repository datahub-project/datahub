import warnings
from typing import Generic, List, Optional, Tuple, TypeVar, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.com.linkedin.pegasus2avro.common import TimeStamp
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass as DatasetProperties,
    EditableDatasetPropertiesClass as EditableDatasetProperties,
    EditableSchemaMetadataClass as EditableSchemaMetadata,
    FineGrainedLineageClass as FineGrainedLineage,
    GlossaryTermAssociationClass as Term,
    KafkaAuditHeaderClass,
    SchemaMetadataClass,
    SystemMetadataClass,
    TagAssociationClass as Tag,
    UpstreamClass as Upstream,
    UpstreamLineageClass as UpstreamLineage,
)
from datahub.metadata.urns import DatasetUrn, TagUrn, Urn
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch
from datahub.specific.aspect_helpers.fine_grained_lineage import (
    HasFineGrainedLineagePatch,
)
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.siblings import HasSiblingsPatch
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.aspect_helpers.terms import HasTermsPatch

_Parent = TypeVar("_Parent", bound=MetadataPatchProposal)


class FieldPatchHelper(Generic[_Parent]):
    def __init__(
        self,
        parent: _Parent,
        field_path: str,
        editable: bool = True,
    ) -> None:
        self._parent: _Parent = parent
        self.field_path = field_path
        self.aspect_name = (
            EditableSchemaMetadata.ASPECT_NAME
            if editable
            else SchemaMetadataClass.ASPECT_NAME
        )
        self.aspect_field = "editableSchemaFieldInfo" if editable else "schemaFieldInfo"

    def add_tag(self, tag: Tag) -> "FieldPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path=(self.aspect_field, self.field_path, "globalTags", "tags", tag.tag),
            value=tag,
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "FieldPatchHelper":
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._parent._add_patch(
            self.aspect_name,
            "remove",
            path=(self.aspect_field, self.field_path, "globalTags", "tags", tag),
            value={},
        )
        return self

    def add_term(self, term: Term) -> "FieldPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path=(
                self.aspect_field,
                self.field_path,
                "glossaryTerms",
                "terms",
                term.urn,
            ),
            value=term,
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "FieldPatchHelper":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._parent._add_patch(
            self.aspect_name,
            "remove",
            path=(self.aspect_field, self.field_path, "glossaryTerms", "terms", term),
            value={},
        )
        return self

    def parent(self) -> _Parent:
        return self._parent


class DatasetPatchBuilder(
    HasOwnershipPatch,
    HasCustomPropertiesPatch,
    HasStructuredPropertiesPatch,
    HasTagsPatch,
    HasTermsPatch,
    HasFineGrainedLineagePatch,
    HasSiblingsPatch,
    MetadataPatchProposal,
):
    def __init__(
        self,
        urn: Union[str, DatasetUrn],
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            str(urn), system_metadata=system_metadata, audit_header=audit_header
        )

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return DatasetProperties.ASPECT_NAME, ("customProperties",)

    @classmethod
    def _fine_grained_lineage_location(cls) -> Tuple[str, PatchPath]:
        return UpstreamLineage.ASPECT_NAME, ("fineGrainedLineages",)

    def add_upstream_lineage(self, upstream: Upstream) -> "DatasetPatchBuilder":
        self._add_patch(
            UpstreamLineage.ASPECT_NAME,
            "add",
            path=("upstreams", upstream.dataset),
            value=upstream,
        )
        return self

    def remove_upstream_lineage(
        self, dataset: Union[str, Urn]
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            UpstreamLineage.ASPECT_NAME,
            "remove",
            path=("upstreams", dataset),
            value={},
        )
        return self

    def set_upstream_lineages(self, upstreams: List[Upstream]) -> "DatasetPatchBuilder":
        self._add_patch(
            UpstreamLineage.ASPECT_NAME, "add", path=("upstreams",), value=upstreams
        )
        return self

    def add_fine_grained_upstream_lineage(
        self, fine_grained_lineage: FineGrainedLineage
    ) -> "DatasetPatchBuilder":
        """
        Deprecated: Use `add_fine_grained_lineage` instead.
        """
        warnings.warn(
            "add_fine_grained_upstream_lineage() is deprecated."
            " Use add_fine_grained_lineage() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.add_fine_grained_lineage(fine_grained_lineage)

    def remove_fine_grained_upstream_lineage(
        self, fine_grained_lineage: FineGrainedLineage
    ) -> "DatasetPatchBuilder":
        """
        Deprecated: Use `remove_fine_grained_lineage` instead.
        """
        warnings.warn(
            "remove_fine_grained_upstream_lineage() is deprecated."
            " Use remove_fine_grained_lineage() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.remove_fine_grained_lineage(fine_grained_lineage)

    def set_fine_grained_upstream_lineages(
        self, fine_grained_lineages: List[FineGrainedLineage]
    ) -> "DatasetPatchBuilder":
        """
        Deprecated: Use `set_fine_grained_lineages` instead.
        """
        warnings.warn(
            "set_fine_grained_upstream_lineages() is deprecated."
            " Use set_fine_grained_lineages() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.set_fine_grained_lineages(fine_grained_lineages)

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
        self, description: Optional[str] = None, editable: bool = False
    ) -> "DatasetPatchBuilder":
        if description is not None:
            self._add_patch(
                (
                    DatasetProperties.ASPECT_NAME
                    if not editable
                    else EditableDatasetProperties.ASPECT_NAME
                ),
                "add",
                path=("description",),
                value=description,
            )
        return self

    def set_display_name(
        self, display_name: Optional[str] = None
    ) -> "DatasetPatchBuilder":
        if display_name is not None:
            self._add_patch(
                DatasetProperties.ASPECT_NAME,
                "add",
                path=("name",),
                value=display_name,
            )
        return self

    def set_qualified_name(
        self, qualified_name: Optional[str] = None
    ) -> "DatasetPatchBuilder":
        if qualified_name is not None:
            self._add_patch(
                DatasetProperties.ASPECT_NAME,
                "add",
                path=("qualifiedName",),
                value=qualified_name,
            )
        return self

    def set_created(
        self, timestamp: Optional[TimeStamp] = None
    ) -> "DatasetPatchBuilder":
        if timestamp is not None:
            self._add_patch(
                DatasetProperties.ASPECT_NAME,
                "add",
                path=("created",),
                value=timestamp,
            )
        return self

    def set_last_modified(
        self, timestamp: Optional[TimeStamp] = None
    ) -> "DatasetPatchBuilder":
        if timestamp is not None:
            self._add_patch(
                DatasetProperties.ASPECT_NAME,
                "add",
                path=("lastModified",),
                value=timestamp,
            )
        return self

    def set_external_url(
        self, external_url: Optional[str] = None
    ) -> "DatasetPatchBuilder":
        if external_url is not None:
            self._add_patch(
                DatasetProperties.ASPECT_NAME,
                "add",
                path=("externalUrl",),
                value=external_url,
            )
        return self
