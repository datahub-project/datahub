import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '@src/types.generated';

export default function useExtractFieldTagsInfo(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    return (record: SchemaField, defaultUneditableTags: GlobalTags | null = null) => {
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidate) =>
            pathMatchesExact(candidate.fieldPath, record.fieldPath),
        );

        const editableTags = editableFieldInfo?.globalTags || { tags: [] };

        // Start with uneditable tags either from default or record
        const uneditableTags: GlobalTags = defaultUneditableTags || record?.globalTags || { tags: [] };

        // Collect extra uneditable tags from path-insensitive matches
        const extraUneditableTags =
            editableSchemaMetadata?.editableSchemaFieldInfo
                .filter((candidate) => pathMatchesInsensitiveToV2(candidate.fieldPath, record.fieldPath))
                .flatMap((info) => info.globalTags?.tags || [])
                .filter(
                    (tag) =>
                        !editableTags?.tags?.some((t) => t.tag.urn === tag.tag.urn) &&
                        !uneditableTags?.tags?.some((t) => t.tag.urn === tag.tag.urn),
                ) || [];

        // Combine all uneditable tags and remove duplicates
        const allUneditableTags = [...(uneditableTags?.tags || []), ...extraUneditableTags];

        // Final deduped uneditable tags excluding any in editableTags
        const filteredUneditableTags = {
            tags: allUneditableTags.filter((tag) => !editableTags?.tags?.some((et) => et.tag.urn === tag.tag.urn)),
        };

        const numberOfTags = (editableTags?.tags?.length ?? 0) + (filteredUneditableTags?.tags?.length ?? 0);

        return {
            editableTags,
            uneditableTags: filteredUneditableTags,
            numberOfTags,
        };
    };
}
