"""Tag-specific mutation classes for tag live testing."""

import datetime
from dataclasses import dataclass
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    TagAssociationClass,
)
from tests.propagation.framework.core.mutations import BaseMutation, FieldMutation


# Tag-specific mutation classes
@dataclass(frozen=True)
class TagAdditionMutation(FieldMutation):
    """Typed mutation for adding a tag to a field."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "tag_addition"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply field tag addition mutation.

        Note: This creates a patch that adds the tag to the field. DataHub will merge
        this with existing editable schema metadata rather than replacing it entirely.
        The key is to only specify the field we're updating - DataHub handles the merge.
        """
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        globalTags=GlobalTagsClass(
                            tags=[TagAssociationClass(tag=self.tag_urn)]
                        ),
                    ),
                ],
                created=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
                lastModified=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
            ),
        )


@dataclass(frozen=True)
class TagUpdateMutation(FieldMutation):
    """Typed mutation for updating a tag on a field (replaces existing tags)."""

    new_tag_urn: str
    old_tag_urn: Optional[str] = None

    def get_mutation_type(self) -> str:
        return "tag_update"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply field tag update mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        globalTags=GlobalTagsClass(
                            tags=[TagAssociationClass(tag=self.new_tag_urn)]
                        ),
                    ),
                ],
                created=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
                lastModified=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
            ),
        )


@dataclass(frozen=True)
class TagRemovalMutation(FieldMutation):
    """Typed mutation for removing a tag from a field."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "tag_removal"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply field tag removal mutation."""
        # For removal, we create an empty GlobalTagsClass or pass empty tag list
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        globalTags=GlobalTagsClass(
                            tags=[]
                        ),  # Empty tags removes all tags
                    ),
                ],
                created=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
                lastModified=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
            ),
        )


@dataclass(frozen=True)
class DatasetTagAdditionMutation(BaseMutation):
    """Typed mutation for adding a tag to a dataset."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "dataset_tag_addition"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply dataset tag addition mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=self.tag_urn)]),
        )


@dataclass(frozen=True)
class DatasetTagRemovalMutation(BaseMutation):
    """Typed mutation for removing a tag from a dataset."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "dataset_tag_removal"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply dataset tag removal mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=GlobalTagsClass(tags=[]),  # Empty tags removes all tags
        )
