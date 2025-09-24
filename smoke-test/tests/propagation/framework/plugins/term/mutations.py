"""Term-specific mutation classes for glossary term live testing."""

import datetime
from dataclasses import dataclass
from typing import List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataChangeProposalClass,
)
from tests.propagation.framework.core.mutations import BaseMutation, FieldMutation


@dataclass
class FieldTermPair:
    """Represents a field-term pair for multi-field mutations."""

    field_name: str
    term_urn: str


# Term-specific mutation classes
@dataclass(frozen=True)
class FieldTermAdditionMutation(FieldMutation):
    """Typed mutation for adding a glossary term to a field."""

    term_urn: str

    def get_mutation_type(self) -> str:
        return "term_addition"

    def explain(self) -> str:
        return f"Adding glossary term {self.term_urn} to field {self.dataset_name}.{self.field_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply field term addition mutation using EditableSchemaMetadataClass."""
        import datetime

        from datahub.metadata.schema_classes import (
            AuditStampClass,
            EditableSchemaFieldInfoClass,
            EditableSchemaMetadataClass,
        )

        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        EditableSchemaFieldInfoClass(
                            fieldPath=self.field_name,
                            glossaryTerms=GlossaryTermsClass(
                                terms=[
                                    GlossaryTermAssociationClass(
                                        urn=self.term_urn,
                                        actor="urn:li:corpuser:test_user",
                                    )
                                ],
                                auditStamp=AuditStampClass(
                                    time=timestamp,
                                    actor="urn:li:corpuser:test_user",
                                ),
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
class FieldTermRemovalMutation(FieldMutation):
    """Typed mutation for removing a glossary term from a field."""

    term_urn: str

    def get_mutation_type(self) -> str:
        return "term_removal"

    def explain(self) -> str:
        return f"Removing glossary term {self.term_urn} from field {self.dataset_name}.{self.field_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalClass]:
        """Apply term removal mutation using DatasetPatchBuilder."""
        from datahub.specific.dataset import DatasetPatchBuilder

        patch_builder = DatasetPatchBuilder(self.dataset_urn)
        patch_builder.for_field(self.field_name).remove_term(self.term_urn)
        return patch_builder.build()


@dataclass(frozen=True)
class FieldTermUpdateMutation(FieldMutation):
    """Typed mutation for updating a glossary term on a field."""

    old_term_urn: str
    new_term_urn: str

    def get_mutation_type(self) -> str:
        return "term_update"

    def explain(self) -> str:
        return f"Updating glossary term on field {self.dataset_name}.{self.field_name} from {self.old_term_urn} to {self.new_term_urn}"

    def apply_mutation(self) -> List[MetadataChangeProposalClass]:
        """Apply term update mutation using DatasetPatchBuilder."""
        from datahub.specific.dataset import DatasetPatchBuilder

        patch_builder = DatasetPatchBuilder(self.dataset_urn)
        field_patch = patch_builder.for_field(self.field_name)
        field_patch.remove_term(self.old_term_urn)
        new_term_association = GlossaryTermAssociationClass(
            urn=self.new_term_urn,
            actor="urn:li:corpuser:foobar",
        )
        field_patch.add_term(new_term_association)
        return patch_builder.build()


@dataclass(frozen=True)
class MultiFieldTermMutation(BaseMutation):
    """Atomic mutation that applies multiple field-term changes in a single MCP.

    This is useful to avoid conflicts when multiple fields need term changes
    that might affect the same downstream propagation targets. This matches
    the behavior of the original test where both field changes are applied
    atomically in a single EditableSchemaMetadataClass.
    """

    field_term_pairs: tuple[FieldTermPair, ...]

    def __post_init__(self) -> None:
        # Convert list to tuple for immutability since we're frozen
        if isinstance(self.field_term_pairs, list):
            object.__setattr__(self, "field_term_pairs", tuple(self.field_term_pairs))

    def get_mutation_type(self) -> str:
        return "multi_field_term_mutation"

    def explain(self) -> str:
        pairs = [f"{pair.field_name}→{pair.term_urn}" for pair in self.field_term_pairs]
        return (
            f"Adding multiple glossary terms to {self.dataset_name}: {', '.join(pairs)}"
        )

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply atomic term mutation with multiple field changes."""
        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        field_infos: List[EditableSchemaFieldInfoClass] = []
        for pair in self.field_term_pairs:
            field_info = EditableSchemaFieldInfoClass(
                fieldPath=pair.field_name,
                glossaryTerms=GlossaryTermsClass(
                    auditStamp=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:foobar",
                    ),
                    terms=[
                        GlossaryTermAssociationClass(
                            urn=pair.term_urn,
                            actor="urn:li:corpuser:foobar",
                        )
                    ],
                ),
            )
            field_infos.append(field_info)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=field_infos,
                    created=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:foobar",
                    ),
                    lastModified=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:foobar",
                    ),
                ),
            )
        ]


# Dataset-level term mutations
@dataclass(frozen=True)
class DatasetTermAdditionMutation(BaseMutation):
    """Typed mutation for adding a glossary term to a dataset."""

    term_urn: str

    def get_mutation_type(self) -> str:
        return "dataset_term_addition"

    def explain(self) -> str:
        return f"Adding glossary term {self.term_urn} to dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset term addition mutation."""
        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=GlossaryTermsClass(
                    auditStamp=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:foobar",
                    ),
                    terms=[
                        GlossaryTermAssociationClass(
                            urn=self.term_urn,
                            actor="urn:li:corpuser:foobar",
                        )
                    ],
                ),
            )
        ]


@dataclass(frozen=True)
class DatasetTermRemovalMutation(BaseMutation):
    """Typed mutation for removing all glossary terms from a dataset."""

    def get_mutation_type(self) -> str:
        return "dataset_term_removal"

    def explain(self) -> str:
        return f"Removing all glossary terms from dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset term removal mutation."""
        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=GlossaryTermsClass(
                    auditStamp=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:foobar",
                    ),
                    terms=[],  # Empty terms removes all terms
                ),
            )
        ]


@dataclass(frozen=True)
class DatasetTermUpdateMutation(BaseMutation):
    """Typed mutation for updating a glossary term on a dataset."""

    old_term_urn: str
    new_term_urn: str

    def get_mutation_type(self) -> str:
        return "dataset_term_update"

    def explain(self) -> str:
        return f"Updating glossary term on dataset {self.dataset_name} from {self.old_term_urn} to {self.new_term_urn}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset term update mutation."""
        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=GlossaryTermsClass(
                    auditStamp=AuditStampClass(
                        time=timestamp,
                        actor="urn:li:corpuser:foobar",
                    ),
                    terms=[
                        GlossaryTermAssociationClass(
                            urn=self.new_term_urn,
                            actor="urn:li:corpuser:foobar",
                        )
                    ],
                ),
            )
        ]
