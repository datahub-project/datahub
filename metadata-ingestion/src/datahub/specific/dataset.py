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
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
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
        (
            transform_op,
            downstream_urn,
            query_id,
        ) = DatasetPatchBuilder.get_fine_grained_key(fine_grained_lineage)
        for upstream_urn in fine_grained_lineage.upstreams or []:
            self._add_patch(
                UpstreamLineage.ASPECT_NAME,
                "add",
                path=self._build_fine_grained_path(
                    transform_op, downstream_urn, query_id, upstream_urn
                ),
                value={"confidenceScore": fine_grained_lineage.confidenceScore},
            )
        return self

    @staticmethod
    def get_fine_grained_key(
        fine_grained_lineage: FineGrainedLineage,
    ) -> Tuple[str, str, str]:
        downstreams = fine_grained_lineage.downstreams or []
        if len(downstreams) != 1:
            raise TypeError("Cannot patch with more or less than one downstream.")
        transform_op = fine_grained_lineage.transformOperation or "NONE"
        downstream_urn = downstreams[0]
        query_id = fine_grained_lineage.query or "NONE"
        return transform_op, downstream_urn, query_id

    @classmethod
    def _build_fine_grained_path(
        cls, transform_op: str, downstream_urn: str, query_id: str, upstream_urn: str
    ) -> PatchPath:
        return (
            "fineGrainedLineages",
            transform_op,
            downstream_urn,
            query_id,
            upstream_urn,
        )

    def remove_fine_grained_upstream_lineage(
        self, fine_grained_lineage: FineGrainedLineage
    ) -> "DatasetPatchBuilder":
        (
            transform_op,
            downstream_urn,
            query_id,
        ) = DatasetPatchBuilder.get_fine_grained_key(fine_grained_lineage)
        for upstream_urn in fine_grained_lineage.upstreams or []:
            self._add_patch(
                UpstreamLineage.ASPECT_NAME,
                "remove",
                path=self._build_fine_grained_path(
                    transform_op, downstream_urn, query_id, upstream_urn
                ),
                value={},
            )
        return self

    def set_fine_grained_upstream_lineages(
        self, fine_grained_lineages: List[FineGrainedLineage]
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            UpstreamLineage.ASPECT_NAME,
            "add",
            path=("fineGrainedLineages",),
            value=fine_grained_lineages,
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
