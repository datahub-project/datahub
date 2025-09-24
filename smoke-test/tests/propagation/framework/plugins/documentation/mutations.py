"""Documentation-specific mutation classes for documentation live testing."""

import datetime
from dataclasses import dataclass
from typing import List, Optional

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
class FieldDocumentationAdditionMutation(FieldMutation):
    """Typed mutation for adding documentation to a field."""

    description: str

    def get_mutation_type(self) -> str:
        return "documentation_addition"

    def explain(self) -> str:
        return f"Adding documentation to field {self.dataset_name}.{self.field_name}: '{self.description}'"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply field documentation addition mutation using EditableSchemaMetadataClass."""
        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        EditableSchemaFieldInfoClass(
                            fieldPath=self.field_name,
                            description=self.description,
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
class FieldDocumentationUpdateMutation(FieldMutation):
    """Typed mutation for updating documentation on a field."""

    new_description: str
    old_description: Optional[str] = None

    def get_mutation_type(self) -> str:
        return "documentation_update"

    def explain(self) -> str:
        return f"Updating documentation on field {self.dataset_name}.{self.field_name} to: '{self.new_description}'"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply field documentation update mutation using field-level DocumentationClass."""
        import time

        from datahub.emitter.mce_builder import make_schema_field_urn
        from datahub.metadata.schema_classes import (
            DocumentationAssociationClass,
            DocumentationClass,
            MetadataAttributionClass,
        )

        field_urn = make_schema_field_urn(self.dataset_urn, self.field_name)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=field_urn,
                aspect=DocumentationClass(
                    documentations=[
                        DocumentationAssociationClass(
                            documentation=self.new_description,
                            attribution=MetadataAttributionClass(
                                time=int(time.time() * 1000),
                                actor="urn:li:corpuser:test_user",
                                source="urn:li:dataHubAction:field_documentation",
                            ),
                        )
                    ]
                ),
            )
        ]


@dataclass(frozen=True)
class FieldDocumentationRemovalMutation(FieldMutation):
    """Typed mutation for removing documentation from a field."""

    def get_mutation_type(self) -> str:
        return "documentation_removal"

    def explain(self) -> str:
        return (
            f"Removing documentation from field {self.dataset_name}.{self.field_name}"
        )

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply field documentation removal mutation using field-level DocumentationClass."""
        from datahub.emitter.mce_builder import make_schema_field_urn
        from datahub.metadata.schema_classes import DocumentationClass

        field_urn = make_schema_field_urn(self.dataset_urn, self.field_name)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=field_urn,
                aspect=DocumentationClass(
                    documentations=[]
                ),  # Empty documentation removes all
            )
        ]


@dataclass(frozen=True)
class DatasetDocumentationAdditionMutation(BaseMutation):
    """Typed mutation for adding documentation to a dataset."""

    description: str

    def get_mutation_type(self) -> str:
        return "dataset_documentation_addition"

    def explain(self) -> str:
        return (
            f"Adding documentation to dataset {self.dataset_name}: '{self.description}'"
        )

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset documentation addition mutation."""
        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableDatasetPropertiesClass(
                    description=self.description,
                    created=AuditStampClass(
                        time=int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp()
                            * 1000
                        ),
                        actor="urn:li:corpuser:test_user",
                    ),
                    lastModified=AuditStampClass(
                        time=int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp()
                            * 1000
                        ),
                        actor="urn:li:corpuser:test_user",
                    ),
                ),
            )
        ]


@dataclass(frozen=True)
class DatasetDocumentationUpdateMutation(BaseMutation):
    """Typed mutation for updating documentation on a dataset."""

    new_description: str
    old_description: Optional[str] = None

    def get_mutation_type(self) -> str:
        return "dataset_documentation_update"

    def explain(self) -> str:
        return f"Updating documentation on dataset {self.dataset_name} to: '{self.new_description}'"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset documentation update mutation."""
        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableDatasetPropertiesClass(
                    description=self.new_description,
                    created=AuditStampClass(
                        time=int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp()
                            * 1000
                        ),
                        actor="urn:li:corpuser:test_user",
                    ),
                    lastModified=AuditStampClass(
                        time=int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp()
                            * 1000
                        ),
                        actor="urn:li:corpuser:test_user",
                    ),
                ),
            )
        ]


@dataclass(frozen=True)
class DatasetDocumentationRemovalMutation(BaseMutation):
    """Typed mutation for removing documentation from a dataset."""

    def get_mutation_type(self) -> str:
        return "dataset_documentation_removal"

    def explain(self) -> str:
        return f"Removing documentation from dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset documentation removal mutation."""
        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableDatasetPropertiesClass(
                    description="",  # Empty string removes documentation
                    created=AuditStampClass(
                        time=int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp()
                            * 1000
                        ),
                        actor="urn:li:corpuser:test_user",
                    ),
                    lastModified=AuditStampClass(
                        time=int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp()
                            * 1000
                        ),
                        actor="urn:li:corpuser:test_user",
                    ),
                ),
            )
        ]
