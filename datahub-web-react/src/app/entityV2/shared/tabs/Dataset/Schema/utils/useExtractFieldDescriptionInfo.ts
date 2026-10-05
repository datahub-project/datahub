import { sanitizeRichText } from '@components/components/Editor/utils';

import { getFieldDescriptionDetails } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import useEditableSchemaFieldInfoMaps, {
    EditableFieldInfoMaps,
} from '@src/app/entityV2/shared/tabs/Dataset/Schema/utils/useEditableSchemaFieldInfoMaps';
import { EditableSchemaMetadata, SchemaField } from '@src/types.generated';

export default function useExtractFieldDescriptionInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    fieldInfoMaps?: EditableFieldInfoMaps,
) {
    const fallbackMaps = useEditableSchemaFieldInfoMaps(fieldInfoMaps ? undefined : editableSchemaMetadata);
    const { exactMap } = fieldInfoMaps ?? fallbackMaps;

    return (record: SchemaField, description: string | undefined | null = null) => {
        const editableFieldInfoB = exactMap.get(record.fieldPath);
        const { displayedDescription, isPropagated, sourceDetail, attribution } = getFieldDescriptionDetails({
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
            attribution,
        };
    };
}
