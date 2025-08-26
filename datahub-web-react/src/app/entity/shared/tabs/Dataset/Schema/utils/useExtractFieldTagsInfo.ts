import { pathMatchesNewPath } from '@src/app/entity/dataset/profile/schema/utils/utils';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
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
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        )?.globalTags;

        const businessAttributeTags =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties?.tags
                ?.tags || [];
        const uneditableTags = {
            tags: [...(defaultUneditableTags?.tags || record?.globalTags?.tags || []), ...businessAttributeTags],
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
            (editableTags?.tags?.length ?? 0) + (uneditableTags?.tags?.length ?? 0) + proposedTags.length;

        return { editableTags, uneditableTags, proposedTags, numberOfTags };
    };
}
