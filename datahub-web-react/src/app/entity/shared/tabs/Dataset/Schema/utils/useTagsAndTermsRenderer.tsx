import React from 'react';
import {
    EditableSchemaFieldInfo,
    EditableSchemaMetadata,
    GlobalTags,
    GlobalTagsUpdate,
    SchemaField,
} from '../../../../../../../types.generated';
import TagTermGroup from '../../../../../../shared/tags/TagTermGroup';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';

export default function useTagsAndTermsRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    onUpdateTags: (update: GlobalTagsUpdate, record?: EditableSchemaFieldInfo) => Promise<any>,
    tagHoveredIndex: string | undefined,
    setTagHoveredIndex: (index: string | undefined) => void,
) {
    const tagAndTermRender = (tags: GlobalTags, record: SchemaField, rowIndex: number | undefined) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        return (
            <TagTermGroup
                uneditableTags={tags}
                editableTags={relevantEditableFieldInfo?.globalTags}
                glossaryTerms={record.glossaryTerms}
                canRemove
                buttonProps={{ size: 'small' }}
                canAdd={tagHoveredIndex === `${record.fieldPath}-${rowIndex}`}
                onOpenModal={() => setTagHoveredIndex(undefined)}
                updateTags={(update) =>
                    onUpdateTags(update, relevantEditableFieldInfo || { fieldPath: record.fieldPath })
                }
            />
        );
    };
    return tagAndTermRender;
}
