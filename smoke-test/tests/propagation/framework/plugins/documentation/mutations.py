"""Documentation-specific mutation classes for documentation live testing."""

import datetime
from dataclasses import dataclass
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableDatasetPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
)
from tests.propagation.framework.core.mutations import BaseMutation, FieldMutation


# Documentation-specific mutation classes
@dataclass(frozen=True)
class DocumentationAdditionMutation(FieldMutation):
    """Typed mutation for adding documentation to a field."""

    description: str

    def get_mutation_type(self) -> str:
        return "documentation_addition"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply field documentation addition mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        description=self.description,
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
class DocumentationUpdateMutation(FieldMutation):
    """Typed mutation for updating documentation on a field."""

    new_description: str
    old_description: Optional[str] = None

    def get_mutation_type(self) -> str:
        return "documentation_update"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply field documentation update mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        description=self.new_description,
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
class DocumentationRemovalMutation(FieldMutation):
    """Typed mutation for removing documentation from a field."""

    def get_mutation_type(self) -> str:
        return "documentation_removal"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply field documentation removal mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        description="",  # Empty string removes documentation
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
class DatasetDocumentationAdditionMutation(BaseMutation):
    """Typed mutation for adding documentation to a dataset."""

    description: str

    def get_mutation_type(self) -> str:
        return "dataset_documentation_addition"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply dataset documentation addition mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableDatasetPropertiesClass(
                description=self.description,
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
class DatasetDocumentationUpdateMutation(BaseMutation):
    """Typed mutation for updating documentation on a dataset."""

    new_description: str
    old_description: Optional[str] = None

    def get_mutation_type(self) -> str:
        return "dataset_documentation_update"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply dataset documentation update mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableDatasetPropertiesClass(
                description=self.new_description,
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
class DatasetDocumentationRemovalMutation(BaseMutation):
    """Typed mutation for removing documentation from a dataset."""

    def get_mutation_type(self) -> str:
        return "dataset_documentation_removal"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply dataset documentation removal mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableDatasetPropertiesClass(
                description="",  # Empty string removes documentation
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
