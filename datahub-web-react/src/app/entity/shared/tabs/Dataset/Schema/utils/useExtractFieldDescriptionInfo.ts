import DOMPurify from 'dompurify';

import { getFieldDescriptionDetails } from '@app/entity/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import { pathMatchesNewPath } from '@src/app/entity/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, SchemaField } from '@src/types.generated';

export default function useExtractFieldDescriptionInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
) {
    return (record: SchemaField, description: string | null = null) => {
        const editableFieldInfoB = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { displayedDescription, isPropagated, sourceDetail, propagatedDescription } = getFieldDescriptionDetails({
            schemaFieldEntity: record.schemaFieldEntity,
            editableFieldInfo: editableFieldInfoB,
            defaultDescription: description || record?.description,
        });

        const sanitizedDescription = DOMPurify.sanitize(displayedDescription);

        return {
            displayedDescription,
            sanitizedDescription,
            isPropagated,
            sourceDetail,
            propagatedDescription,
        };
    };
}
