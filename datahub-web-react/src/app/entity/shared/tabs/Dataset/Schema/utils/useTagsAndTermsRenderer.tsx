import React from 'react';
import { EditableSchemaMetadata, EntityType, GlobalTags, SchemaField } from '../../../../../../../types.generated';
import TagTermGroup from '../../../../../../shared/tags/TagTermGroup';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useSchemaRefetch } from '../SchemaContext';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';

export default function useTagsAndTermsRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    options: { showTags: boolean; showTerms: boolean },
    filterText: string,
    canEdit: boolean,
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    const tagAndTermRender = (tags: GlobalTags, record: SchemaField) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        const newRecord = { ...record };

        if (!newRecord.glossaryTerms) {
            newRecord.glossaryTerms = { terms: [] };
        }

        if (!newRecord.glossaryTerms.terms) {
            newRecord.glossaryTerms.terms = [];
        }

        if (
            relevantEditableFieldInfo?.businessAttributes?.businessAttribute?.businessAttribute?.properties
                ?.glossaryTerms?.terms
        ) {
            newRecord.glossaryTerms.terms = [
                ...newRecord.glossaryTerms.terms,
                ...relevantEditableFieldInfo.businessAttributes.businessAttribute.businessAttribute.properties
                    .glossaryTerms.terms,
            ];
        }
        let newTags = {};
        if (
            relevantEditableFieldInfo?.businessAttributes?.businessAttribute?.businessAttribute?.properties?.tags?.tags
        ) {
            newTags = {
                ...tags,
                tags: [
                    ...(tags?.tags || []),
                    ...relevantEditableFieldInfo?.businessAttributes?.businessAttribute?.businessAttribute?.properties
                        ?.tags?.tags,
                ],
            };
        }
        return (
            <TagTermGroup
                uneditableTags={options.showTags ? newTags : null}
                editableTags={options.showTags ? relevantEditableFieldInfo?.globalTags : null}
                uneditableGlossaryTerms={options.showTerms ? newRecord.glossaryTerms : null}
                editableGlossaryTerms={options.showTerms ? relevantEditableFieldInfo?.glossaryTerms : null}
                canRemove={canEdit}
                buttonProps={{ size: 'small' }}
                canAddTag={canEdit && options.showTags}
                canAddTerm={canEdit && options.showTerms}
                entityUrn={urn}
                entityType={EntityType.Dataset}
                entitySubresource={record.fieldPath}
                highlightText={filterText}
                refetch={refresh}
            />
        );
    };
    return tagAndTermRender;
}
