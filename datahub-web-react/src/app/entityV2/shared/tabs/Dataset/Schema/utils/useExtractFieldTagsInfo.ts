import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { getProposedItemsByType } from '@src/app/entityV2/shared/utils';
import { findFieldPathProposal } from '@src/app/shared/tags/utils/proposalUtils';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import {
    ActionRequest,
    ActionRequestType,
    EditableSchemaMetadata,
    GlobalTags,
    SchemaField,
} from '@src/types.generated';

export default function useExtractFieldTagsInfo(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

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

        const proposedTags: ActionRequest[] =
            findFieldPathProposal(
                getProposedItemsByType(
                    (baseEntity?.dataset?.proposals || []) as ActionRequest[],
                    ActionRequestType.TagAssociation,
                ) || [],
                record.fieldPath,
            ) ?? [];

        const numberOfTags =
            (editableTags?.tags?.length ?? 0) + (filteredUneditableTags?.tags?.length ?? 0) + proposedTags.length;

        return {
            editableTags,
            uneditableTags: filteredUneditableTags,
            proposedTags,
            numberOfTags,
        };
    };
}
