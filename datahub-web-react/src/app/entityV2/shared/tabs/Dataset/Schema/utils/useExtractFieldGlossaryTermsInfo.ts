import { pathMatchesExact } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, GlossaryTerms, SchemaField } from '@src/types.generated';

export default function useExtractFieldGlossaryTermsInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
) {
    return (record: SchemaField, defaultUneditableTerms: GlossaryTerms | null = null) => {
        const editableTerms = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        )?.glossaryTerms;

        const uneditableTerms = defaultUneditableTerms || record?.glossaryTerms;

        const numberOfTerms = (editableTerms?.terms?.length ?? 0) + (uneditableTerms?.terms?.length ?? 0);

        return { editableTerms, uneditableTerms, numberOfTerms };
    };
}
