import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '@src/types.generated';

export default function useExtractFieldTagsInfo(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    return (record: SchemaField, defaultUneditableTags: GlobalTags | null = null) => {
        const editableTags = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        )?.globalTags;

        const uneditableTags: GlobalTags = defaultUneditableTags || record?.globalTags || {};

        // Add tags from other fields that match the field path insensitive to V2
        const extraUneditableTags = editableSchemaMetadata?.editableSchemaFieldInfo
            .filter((candidateEditableFieldInfo) =>
                pathMatchesInsensitiveToV2(candidateEditableFieldInfo.fieldPath, record.fieldPath),
            )
            .flatMap((el) => el.globalTags?.tags || [])
            // Filter out tags that are already in the uneditableTags or editableTags
            .filter(
                (tag) =>
                    !uneditableTags?.tags?.some((t) => t.tag.urn === tag.tag.urn) &&
                    !editableTags?.tags?.some((t) => t.tag.urn === tag.tag.urn),
            );
        if (extraUneditableTags?.length) {
            uneditableTags.tags = [...(uneditableTags?.tags || []), ...extraUneditableTags];
        }

        const numberOfTags = (editableTags?.tags?.length ?? 0) + (uneditableTags?.tags?.length ?? 0);

        return { editableTags, uneditableTags, numberOfTags };
    };
}
