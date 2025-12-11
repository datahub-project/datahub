/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

        // Combine all uneditable terms and remove duplicates
        const allUneditableTerms = [...baseUneditableTerms, ...extraUneditableTerms];

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
