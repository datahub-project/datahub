"""Tag-specific mutation classes for tag live testing."""

import datetime
from dataclasses import dataclass
from typing import List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    MetadataChangeProposalClass,
    TagAssociationClass,
)
from tests.propagation.framework.core.mutations import BaseMutation, FieldMutation


# Tag-specific mutation classes
@dataclass(frozen=True)
class FieldTagAdditionMutation(FieldMutation):
    """Typed mutation for adding a tag to a field."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "tag_addition"

    def explain(self) -> str:
        return (
            f"Adding tag {self.tag_urn} to field {self.dataset_name}.{self.field_name}"
        )

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply field tag addition mutation using EditableSchemaMetadataClass."""

        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        EditableSchemaFieldInfoClass(
                            fieldPath=self.field_name,
                            globalTags=GlobalTagsClass(
                                tags=[TagAssociationClass(tag=self.tag_urn)]
                            ),
                        )
                    ],
                    created=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:test_user",
                    ),
                    lastModified=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:test_user",
                    ),
                ),
            )
        ]


@dataclass(frozen=True)
class FieldTagUpdateMutation(FieldMutation):
    """Typed mutation for updating a tag on a field (replaces existing tags)."""

    tag_urn: str
    old_tag_urn: Optional[str] = None

    def get_mutation_type(self) -> str:
        return "tag_update"

    def explain(self) -> str:
        if self.old_tag_urn:
            return f"Updating tag on field {self.dataset_name}.{self.field_name} from {self.old_tag_urn} to {self.tag_urn}"
        return (
            f"Setting tag {self.tag_urn} on field {self.dataset_name}.{self.field_name}"
        )

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply field tag update mutation using EditableSchemaMetadataClass."""

        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        EditableSchemaFieldInfoClass(
                            fieldPath=self.field_name,
                            globalTags=GlobalTagsClass(
                                tags=[TagAssociationClass(tag=self.tag_urn)]
                            ),
                        )
                    ],
                    created=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:test_user",
                    ),
                    lastModified=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:test_user",
                    ),
                ),
            )
        ]


@dataclass(frozen=True)
class FieldTagRemovalMutation(FieldMutation):
    """Typed mutation for removing a tag from a field."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "tag_removal"

    def explain(self) -> str:
        return f"Removing tag {self.tag_urn} from field {self.dataset_name}.{self.field_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalClass]:
        """Apply field tag removal mutation using DatasetPatchBuilder."""
        from datahub.specific.dataset import DatasetPatchBuilder

        patch_builder = DatasetPatchBuilder(self.dataset_urn)
        patch_builder.for_field(self.field_name).remove_tag(self.tag_urn)
        return patch_builder.build()


@dataclass(frozen=True)
class DatasetTagAdditionMutation(BaseMutation):
    """Typed mutation for adding a tag to a dataset."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "dataset_tag_addition"

    def explain(self) -> str:
        return f"Adding tag {self.tag_urn} to dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset tag addition mutation."""
        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=self.tag_urn)]),
            )
        ]


@dataclass(frozen=True)
class DatasetTagRemovalMutation(BaseMutation):
    """Typed mutation for removing a tag from a dataset."""

    tag_urn: str

    def get_mutation_type(self) -> str:
        return "dataset_tag_removal"

    def explain(self) -> str:
        return f"Removing tag {self.tag_urn} from dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset tag removal mutation."""
        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=GlobalTagsClass(tags=[]),  # Empty tags removes all tags
            )
        ]
