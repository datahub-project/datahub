import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { pathMatchesExact } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { findFieldPathProposal } from '@src/app/shared/tags/utils/proposalUtils';
import { ActionRequest, EditableSchemaMetadata, GlobalTags, SchemaField } from '@src/types.generated';

export default function useExtractFieldTagsInfo(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    const baseEntity = useBaseEntity();

    return (record: SchemaField, defaultUneditableTags: GlobalTags | null = null) => {
        const editableTags = editableSchemaMetadata?.editableSchemaFieldInfo.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        )?.globalTags;

        const uneditableTags = defaultUneditableTags || record?.globalTags;

        const proposedTags: ActionRequest[] =
            findFieldPathProposal(
                // eslint-disable-next-line
                // @ts-ignore
                // eslint-disable-next-line
                baseEntity?.['dataset']?.['tagProposals'] || [],
                record.fieldPath,
            ) ?? [];

        const numberOfTags =
            (editableTags?.tags?.length ?? 0) + (uneditableTags?.tags?.length ?? 0) + proposedTags.length;

        return { editableTags, uneditableTags, proposedTags, numberOfTags };
    };
}
