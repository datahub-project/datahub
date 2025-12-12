import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, GlossaryTerms, SchemaField } from '@src/types.generated';

type ReturnValue = {
    directTerms?: GlossaryTerms;
    editableTerms?: GlossaryTerms;
    uneditableTerms?: GlossaryTerms;
    numberOfTerms: number;
};
type ReturnType = (record: SchemaField, defaultUneditableTerms?: GlossaryTerms | null) => ReturnValue;

export default function useExtractFieldGlossaryTermsInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
): ReturnType {
    return (record: SchemaField, defaultUneditableTerms: GlossaryTerms | null = null) => {
        // Three term locations: schema field entity, EditableSchemaMetadata, SchemaMetadata (uneditable)
        const schemaFieldTerms = record?.schemaFieldEntity?.glossaryTerms?.terms || [];
        const schemaFieldTermUrns = new Set(schemaFieldTerms.map((t) => t.term.urn));

        // Extract business attribute glossary terms
        const businessAttributeTerms =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties
                ?.glossaryTerms?.terms || [];

        // Editable terms: from EditableSchemaMetadata and not on schema field entity itself
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidate) =>
            pathMatchesExact(candidate.fieldPath, record.fieldPath),
        );
        const baseEditableTerms = editableFieldInfo?.glossaryTerms?.terms || [];
        const editableTerms = baseEditableTerms.filter((t) => !schemaFieldTermUrns.has(t.term.urn));
        const editableTermUrns = new Set(editableTerms.map((t) => t.term.urn));

        // Uneditable terms: from SchemaMetadata and not in EditableSchemaMetadata or on schema field entity
        // Also includes terms referenced by EditableSchemaMetadata with a field path that does not exactly match,
        // but is functionally the same (i.e. v1 <-> v2 equivalent). These in practice are not editable
        // because they're technically on a different field path
        const baseUneditableTerms = defaultUneditableTerms?.terms || record?.glossaryTerms?.terms || [];
        const baseUneditableTermUrns = new Set(baseUneditableTerms.map((t) => t.term.urn));

        // Collect extra uneditable terms from path-insensitive matches
        const extraUneditableTerms =
            editableSchemaMetadata?.editableSchemaFieldInfo
                .filter((candidate) => pathMatchesInsensitiveToV2(candidate.fieldPath, record.fieldPath))
                .flatMap((info) => info.glossaryTerms?.terms || [])
                .filter((t) => !baseUneditableTermUrns.has(t.term.urn)) || [];

        // Combine all uneditable terms including business attribute terms and remove duplicates
        const allUneditableTerms = [...baseUneditableTerms, ...extraUneditableTerms, ...businessAttributeTerms];

        // Final deduped uneditable terms excluding any in editableTerms
        const uneditableTerms = allUneditableTerms.filter(
            (t) => !schemaFieldTermUrns.has(t.term.urn) && !editableTermUrns.has(t.term.urn),
        );

        return {
            directTerms: { terms: schemaFieldTerms },
            editableTerms: { terms: editableTerms },
            uneditableTerms: { terms: uneditableTerms },
            numberOfTerms: schemaFieldTerms.length + editableTerms.length + uneditableTerms.length,
        };
    };
}
