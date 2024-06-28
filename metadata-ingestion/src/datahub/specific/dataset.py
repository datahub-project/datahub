from typing import Dict, Generic, List, Optional, Tuple, TypeVar, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.com.linkedin.pegasus2avro.common import TimeStamp
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass as DatasetProperties,
    EditableDatasetPropertiesClass as EditableDatasetProperties,
    EditableSchemaMetadataClass as EditableSchemaMetadata,
    FineGrainedLineageClass as FineGrainedLineage,
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
from datahub.specific.structured_properties import StructuredPropertiesPatchHelper
from datahub.utilities.urns.tag_urn import TagUrn
from datahub.utilities.urns.urn import Urn

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

    def parent(self) -> _Parent:
        return self._parent


class DatasetPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )
        self.custom_properties_patch_helper = CustomPropertiesPatchHelper(
            self, DatasetProperties.ASPECT_NAME
        )
        self.ownership_patch_helper = OwnershipPatchHelper(self)
        self.structured_properties_patch_helper = StructuredPropertiesPatchHelper(self)

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
            path=f"/upstreams/{self.quote(upstream.dataset)}",
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
            UpstreamLineage.ASPECT_NAME, "add", path="/upstreams", value=upstreams
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
                path=DatasetPatchBuilder.quote_fine_grained_path(
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
    def quote_fine_grained_path(
        cls, transform_op: str, downstream_urn: str, query_id: str, upstream_urn: str
    ) -> str:
        return (
            f"/fineGrainedLineages/{cls.quote(transform_op)}/"
            f"{cls.quote(downstream_urn)}/{cls.quote(query_id)}/{cls.quote(upstream_urn)}"
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
                path=DatasetPatchBuilder.quote_fine_grained_path(
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
            path="/fineGrainedLineages",
            value=fine_grained_lineages,
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
                path="/description",
                value=description,
            )
        return self

    def set_custom_properties(
        self, custom_properties: Dict[str, str]
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            DatasetProperties.ASPECT_NAME,
            "add",
            path="/customProperties",
            value=custom_properties,
        )
        return self

    def add_custom_property(self, key: str, value: str) -> "DatasetPatchBuilder":
        self.custom_properties_patch_helper.add_property(key, value)
        return self

    def add_custom_properties(
        self, custom_properties: Optional[Dict[str, str]] = None
    ) -> "DatasetPatchBuilder":
        if custom_properties is not None:
            for key, value in custom_properties.items():
                self.custom_properties_patch_helper.add_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> "DatasetPatchBuilder":
        self.custom_properties_patch_helper.remove_property(key)
        return self

    def set_display_name(
        self, display_name: Optional[str] = None
    ) -> "DatasetPatchBuilder":
        if display_name is not None:
            self._add_patch(
                DatasetProperties.ASPECT_NAME,
                "add",
                path="/name",
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
                path="/qualifiedName",
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
                path="/created",
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
                path="/lastModified",
                value=timestamp,
            )
        return self

    def set_structured_property(
        self, property_name: str, value: Union[str, float, List[Union[str, float]]]
    ) -> "DatasetPatchBuilder":
        """
        This is a helper method to set a structured property.
        @param property_name: the name of the property (either bare or urn form)
        @param value: the value of the property (for multi-valued properties, this can be a list)
        """
        self.structured_properties_patch_helper.set_property(property_name, value)
        return self

    def add_structured_property(
        self, property_name: str, value: Union[str, float]
    ) -> "DatasetPatchBuilder":
        """
        This is a helper method to add a structured property.
        @param property_name: the name of the property (either bare or urn form)
        @param value: the value of the property (for multi-valued properties, this value will be appended to the list)
        """
        self.structured_properties_patch_helper.add_property(property_name, value)
        return self

    def remove_structured_property(self, property_name: str) -> "DatasetPatchBuilder":
        """
        This is a helper method to remove a structured property.
        @param property_name: the name of the property (either bare or urn form)
        """
        self.structured_properties_patch_helper.remove_property(property_name)
        return self
