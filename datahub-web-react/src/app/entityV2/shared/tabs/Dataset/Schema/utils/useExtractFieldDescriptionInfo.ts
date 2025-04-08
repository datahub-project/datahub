import { getFieldDescriptionDetails } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import { sanitizeRichText } from '@app/entityV2/shared/tabs/Documentation/components/editor/utils';
import { pathMatchesExact } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, SchemaField } from '@src/types.generated';

export default function useExtractFieldDescriptionInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
) {
    return (record: SchemaField, description: string | undefined | null = null) => {
        const editableFieldInfoB = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { displayedDescription, isPropagated, sourceDetail } = getFieldDescriptionDetails({
            schemaFieldEntity: record.schemaFieldEntity,
            editableFieldInfo: editableFieldInfoB,
            defaultDescription: description || record?.description,
        });

        const sanitizedDescription = sanitizeRichText(displayedDescription);

        return {
            displayedDescription,
            sanitizedDescription,
            isPropagated,
            sourceDetail,
        };
    };
}
