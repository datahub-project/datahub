import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '@src/types.generated';

type ReturnValue = {
    directTags?: GlobalTags;
    editableTags?: GlobalTags;
    uneditableTags?: GlobalTags;
    numberOfTags: number;
};
type ReturnType = (record: SchemaField, defaultUneditableTags?: GlobalTags | null) => ReturnValue;

export default function useExtractFieldTagsInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
): ReturnType {
    return (record: SchemaField, defaultUneditableTags: GlobalTags | null = null) => {
        // Three tag locations: schema field entity, EditableSchemaMetadata, SchemaMetadata (uneditable)
        const schemaFieldTags = record?.schemaFieldEntity?.tags?.tags || [];
        const schemaFieldTagUrns = new Set(schemaFieldTags.map((t) => t.tag.urn));

        // Extract business attribute tags
        const businessAttributeTags =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties?.tags
                ?.tags || [];

        // Editable tags: from EditableSchemaMetadata and not on schema field entity itself
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidate) =>
            pathMatchesExact(candidate.fieldPath, record.fieldPath),
        );
        const baseEditableTags = editableFieldInfo?.globalTags?.tags || [];
        const editableTags = baseEditableTags.filter((tag) => !schemaFieldTagUrns.has(tag.tag.urn));
        const editableTagUrns = new Set(editableTags.map((t) => t.tag.urn));

        // Uneditable tags: from SchemaMetadata and not in EditableSchemaMetadata or on schema field entity
        // Also includes tags referenced by EditableSchemaMetadata with a field path that does not exactly match,
        // but is functionally the same (i.e. v1 <-> v2 equivalent). These in practice are not editable
        // because they're technically on a different field path
        const baseUneditableTags = defaultUneditableTags?.tags || record?.globalTags?.tags || [];
        const baseUneditableTagUrns = new Set(baseUneditableTags.map((t) => t.tag.urn));

        // Collect extra uneditable tags from path-insensitive matches
        const extraUneditableTags =
            editableSchemaMetadata?.editableSchemaFieldInfo
                .filter((candidate) => pathMatchesInsensitiveToV2(candidate.fieldPath, record.fieldPath))
                .flatMap((info) => info.globalTags?.tags || [])
                .filter((tag) => !baseUneditableTagUrns.has(tag.tag.urn)) || [];

        // Combine all uneditable tags including business attribute tags and remove duplicates
        const allUneditableTags = [...baseUneditableTags, ...extraUneditableTags, ...businessAttributeTags];

        // Final deduped uneditable tags excluding any in editableTags
        const uneditableTags = allUneditableTags.filter(
            (tag) => !schemaFieldTagUrns.has(tag.tag.urn) && !editableTagUrns.has(tag.tag.urn),
        );

        return {
            directTags: { tags: schemaFieldTags },
            editableTags: { tags: editableTags },
            uneditableTags: { tags: uneditableTags },
            numberOfTags: schemaFieldTags.length + editableTags.length + uneditableTags.length,
        };
    };
}
