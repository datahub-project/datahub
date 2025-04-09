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

        const proposedTags: ActionRequest[] =
            findFieldPathProposal(
                getProposedItemsByType(
                    (baseEntity?.dataset?.proposals || []) as ActionRequest[],
                    ActionRequestType.TagAssociation,
                ) || [],
                record.fieldPath,
            ) ?? [];

        const numberOfTags =
            (editableTags?.tags?.length ?? 0) + (uneditableTags?.tags?.length ?? 0) + proposedTags.length;

        return { editableTags, uneditableTags, proposedTags, numberOfTags };
    };
}
