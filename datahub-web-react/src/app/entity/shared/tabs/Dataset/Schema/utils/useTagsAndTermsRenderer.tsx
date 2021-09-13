import React from 'react';
import {
    EditableSchemaFieldInfo,
    EditableSchemaMetadata,
    EntityType,
    GlobalTags,
    GlobalTagsUpdate,
    SchemaField,
} from '../../../../../../../types.generated';
import TagTermGroup from '../../../../../../shared/tags/TagTermGroup';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useEntityData, useRefetch } from '../../../../EntityContext';

export default function useTagsAndTermsRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    onUpdateTags: (update: GlobalTagsUpdate, record?: EditableSchemaFieldInfo) => Promise<any>,
    tagHoveredIndex: string | undefined,
    setTagHoveredIndex: (index: string | undefined) => void,
) {
    const { urn } = useEntityData();
    const refetch = useRefetch();

    const tagAndTermRender = (tags: GlobalTags, record: SchemaField, rowIndex: number | undefined) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        return (
            <TagTermGroup
                uneditableTags={tags}
                editableTags={relevantEditableFieldInfo?.globalTags}
                uneditableGlossaryTerms={record.glossaryTerms}
                editableGlossaryTerms={relevantEditableFieldInfo?.glossaryTerms}
                canRemove
                buttonProps={{ size: 'small' }}
                canAddTag={tagHoveredIndex === `${record.fieldPath}-${rowIndex}`}
                canAddTerm={tagHoveredIndex === `${record.fieldPath}-${rowIndex}`}
                onOpenModal={() => setTagHoveredIndex(undefined)}
                entityUrn={urn}
                entityType={EntityType.Dataset}
                entitySubresource={record.fieldPath}
                refetch={refetch}
            />
        );
    };
    return tagAndTermRender;
}
