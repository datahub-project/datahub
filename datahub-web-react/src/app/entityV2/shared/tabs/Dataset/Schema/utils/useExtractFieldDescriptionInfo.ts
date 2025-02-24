import { pathMatchesExact } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { useIsDocumentationInferenceEnabled } from '@src/app/entityV2/shared/components/inferredDocs/utils';
import { EditableSchemaMetadata, SchemaField } from '@src/types.generated';
import { getFieldDescriptionDetails } from './getFieldDescriptionDetails';
import { sanitizeRichText } from '../../../Documentation/components/editor/utils';

export default function useExtractFieldDescriptionInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
) {
    const enableInferredDescriptions = useIsDocumentationInferenceEnabled();

    return (record: SchemaField, description: string | undefined | null = null) => {
        const editableFieldInfoB = editableSchemaMetadata?.editableSchemaFieldInfo.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { displayedDescription, isPropagated, isInferred, sourceDetail } = getFieldDescriptionDetails({
            schemaFieldEntity: record.schemaFieldEntity,
            editableFieldInfo: editableFieldInfoB,
            defaultDescription: description || record?.description,
            enableInferredDescriptions,
        });

        const sanitizedDescription = sanitizeRichText(displayedDescription);

        return {
            displayedDescription,
            sanitizedDescription,
            isPropagated,
            isInferred,
            sourceDetail,
        };
    };
}
