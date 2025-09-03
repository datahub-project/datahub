"""Term-specific mutation classes for glossary term live testing."""

import datetime
from dataclasses import dataclass

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)
from tests.propagation.framework.core.mutations import FieldMutation


# Term-specific mutation classes
@dataclass(frozen=True)
class TermAdditionMutation(FieldMutation):
    """Typed mutation for adding a glossary term to a field."""

    term_urn: str

    def get_mutation_type(self) -> str:
        return "term_addition"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply term addition mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        glossaryTerms=GlossaryTermsClass(
                            auditStamp=AuditStampClass(
                                time=int(
                                    datetime.datetime.now(
                                        datetime.timezone.utc
                                    ).timestamp()
                                    * 1000
                                ),
                                actor="urn:li:corpuser:test_user",
                            ),
                            terms=[
                                GlossaryTermAssociationClass(
                                    urn=self.term_urn,
                                    actor="urn:li:corpuser:test_user",
                                )
                            ],
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
class TermRemovalMutation(FieldMutation):
    """Typed mutation for removing a glossary term from a field."""

    def get_mutation_type(self) -> str:
        return "term_removal"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply term removal mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        glossaryTerms=GlossaryTermsClass(
                            auditStamp=AuditStampClass(
                                time=int(
                                    datetime.datetime.now(
                                        datetime.timezone.utc
                                    ).timestamp()
                                    * 1000
                                ),
                                actor="urn:li:corpuser:test_user",
                            ),
                            terms=[],  # Empty terms removes all terms
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
class TermUpdateMutation(FieldMutation):
    """Typed mutation for updating a glossary term on a field."""

    old_term_urn: str
    new_term_urn: str

    def get_mutation_type(self) -> str:
        return "term_update"

    def apply_mutation(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Apply term update mutation."""
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=self.field_name,
                        glossaryTerms=GlossaryTermsClass(
                            auditStamp=AuditStampClass(
                                time=int(
                                    datetime.datetime.now(
                                        datetime.timezone.utc
                                    ).timestamp()
                                    * 1000
                                ),
                                actor="urn:li:corpuser:test_user",
                            ),
                            terms=[
                                GlossaryTermAssociationClass(
                                    urn=self.new_term_urn,
                                    actor="urn:li:corpuser:test_user",
                                )
                            ],
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
