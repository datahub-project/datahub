import { pathMatchesExact } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '@src/types.generated';

export default function useExtractFieldTagsInfo(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    return (record: SchemaField, defaultUneditableTags: GlobalTags | null = null) => {
        const editableTags = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        )?.globalTags;

        const uneditableTags = defaultUneditableTags || record?.globalTags;

        const numberOfTags = (editableTags?.tags?.length ?? 0) + (uneditableTags?.tags?.length ?? 0);

        return { editableTags, uneditableTags, numberOfTags };
    };
}
