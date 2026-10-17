from typing import Optional, Tuple, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentContentsClass,
    DocumentInfoClass,
    DocumentSourceClass,
    DocumentStatusClass,
    KafkaAuditHeaderClass,
    ParentDocumentClass,
    RelatedAssetClass,
    RelatedDocumentClass,
    SystemMetadataClass,
)
from datahub.metadata.urns import Urn
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch


class DocumentPatchBuilder(HasCustomPropertiesPatch, MetadataPatchProposal):
    """Patches a Document's documentInfo aspect field by field.

    Related assets and related documents are keyed by URN, so adding or removing
    one leaves the others in place, including those users added in DataHub.

    Requires a DataHub server that advertises
    ``ServiceFeature.DOCUMENT_INFO_PATCH``; older servers reject documentInfo
    patches.
    """

    def __init__(
        self,
        urn: Union[str, Urn],
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            str(urn),
            system_metadata=system_metadata,
            audit_header=audit_header,
        )

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return DocumentInfoClass.ASPECT_NAME, ("customProperties",)

    def _set_field(
        self,
        field: str,
        value: Union[
            str,
            AuditStampClass,
            DocumentContentsClass,
            DocumentSourceClass,
            DocumentStatusClass,
            ParentDocumentClass,
        ],
    ) -> None:
        self._add_patch(
            DocumentInfoClass.ASPECT_NAME, "add", path=(field,), value=value
        )

    def set_title(self, title: str) -> "DocumentPatchBuilder":
        self._set_field("title", title)
        return self

    def set_source(self, source: DocumentSourceClass) -> "DocumentPatchBuilder":
        self._set_field("source", source)
        return self

    def set_status(self, status: DocumentStatusClass) -> "DocumentPatchBuilder":
        self._set_field("status", status)
        return self

    def set_contents(self, contents: DocumentContentsClass) -> "DocumentPatchBuilder":
        self._set_field("contents", contents)
        return self

    def set_created(self, created: AuditStampClass) -> "DocumentPatchBuilder":
        self._set_field("created", created)
        return self

    def set_last_modified(
        self, last_modified: AuditStampClass
    ) -> "DocumentPatchBuilder":
        self._set_field("lastModified", last_modified)
        return self

    def set_parent_document(
        self, parent_document_urn: Union[str, Urn]
    ) -> "DocumentPatchBuilder":
        self._set_field(
            "parentDocument", ParentDocumentClass(document=str(parent_document_urn))
        )
        return self

    def remove_parent_document(self) -> "DocumentPatchBuilder":
        self._add_patch(
            DocumentInfoClass.ASPECT_NAME, "remove", path=("parentDocument",), value={}
        )
        return self

    def add_related_asset(self, asset_urn: Union[str, Urn]) -> "DocumentPatchBuilder":
        self._add_patch(
            DocumentInfoClass.ASPECT_NAME,
            "add",
            path=("relatedAssets", asset_urn),
            value=RelatedAssetClass(asset=str(asset_urn)),
        )
        return self

    def remove_related_asset(
        self, asset_urn: Union[str, Urn]
    ) -> "DocumentPatchBuilder":
        self._add_patch(
            DocumentInfoClass.ASPECT_NAME,
            "remove",
            path=("relatedAssets", asset_urn),
            value={},
        )
        return self

    def add_related_document(
        self, document_urn: Union[str, Urn]
    ) -> "DocumentPatchBuilder":
        self._add_patch(
            DocumentInfoClass.ASPECT_NAME,
            "add",
            path=("relatedDocuments", document_urn),
            value=RelatedDocumentClass(document=str(document_urn)),
        )
        return self

    def remove_related_document(
        self, document_urn: Union[str, Urn]
    ) -> "DocumentPatchBuilder":
        self._add_patch(
            DocumentInfoClass.ASPECT_NAME,
            "remove",
            path=("relatedDocuments", document_urn),
            value={},
        )
        return self
