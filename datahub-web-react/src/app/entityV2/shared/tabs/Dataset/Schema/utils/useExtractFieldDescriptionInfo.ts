import { useMemo } from 'react';

import { sanitizeRichText } from '@components/components/Editor/utils';

import { getFieldDescriptionDetails } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import { EditableSchemaFieldInfo, EditableSchemaMetadata, SchemaField } from '@src/types.generated';

export default function useExtractFieldDescriptionInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
) {
    // Build a fieldPath → editableFieldInfo map once per editableSchemaMetadata identity.
    // Replaces the O(n) .find() scan that previously ran on every rendered row.
    const editableFieldInfoByPath = useMemo(() => {
        const map = new Map<string, EditableSchemaFieldInfo>();
        (editableSchemaMetadata?.editableSchemaFieldInfo ?? []).forEach((info) => {
            if (!map.has(info.fieldPath)) map.set(info.fieldPath, info);
        });
        return map;
    }, [editableSchemaMetadata]);

    return (record: SchemaField, description: string | undefined | null = null) => {
        const editableFieldInfoB = editableFieldInfoByPath.get(record.fieldPath);
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
