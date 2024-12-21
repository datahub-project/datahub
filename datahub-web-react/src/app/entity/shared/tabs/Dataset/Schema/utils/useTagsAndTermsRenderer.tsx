import React from 'react';
import { EditableSchemaMetadata, EntityType, GlobalTags, SchemaField } from '../../../../../../../types.generated';
import TagTermGroup from '../../../../../../shared/tags/TagTermGroup';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import useExtractFieldGlossaryTermsInfo from './useExtractFieldGlossaryTermsInfo';
import useExtractFieldTagsInfo from './useExtractFieldTagsInfo';

export default function useTagsAndTermsRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    options: { showTags: boolean; showTerms: boolean },
    filterText: string,
    canEdit: boolean,
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const extractFieldGlossaryTermsInfo = useExtractFieldGlossaryTermsInfo(editableSchemaMetadata);
    const extractFieldTagsInfo = useExtractFieldTagsInfo(editableSchemaMetadata);

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    const tagAndTermRender = (tags: GlobalTags, record: SchemaField) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        const businessAttributeTags =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties?.tags
                ?.tags || [];
        const businessAttributeTerms =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties
                ?.glossaryTerms?.terms || [];

        return (
            <TagTermGroup
                uneditableTags={options.showTags ? uneditableTags : null}
                editableTags={options.showTags ? editableTags : null}
                uneditableGlossaryTerms={options.showTerms ? uneditableTerms : null}
                editableGlossaryTerms={options.showTerms ? editableTerms : null}
                canRemove={canEdit}
                buttonProps={{ size: 'small' }}
                canAddTag={canEdit && options.showTags}
                canAddTerm={canEdit && options.showTerms}
                entityUrn={urn}
                entityType={EntityType.Dataset}
                entitySubresource={record.fieldPath}
                highlightText={filterText}
                refetch={refresh}
                proposedGlossaryTerms={options.showTerms ? proposedTerms : []}
                proposedTags={options.showTags ? proposedTags : []}
            />
        );
    };
    return tagAndTermRender;
}
