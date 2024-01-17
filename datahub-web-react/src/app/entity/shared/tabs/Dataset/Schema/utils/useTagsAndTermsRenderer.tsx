import React from 'react';
import { EditableSchemaMetadata, EntityType, GlobalTags, SchemaField } from '../../../../../../../types.generated';
import TagTermGroup from '../../../../../../shared/tags/TagTermGroup';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';

export default function useTagsAndTermsRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    tagHoveredIndex: string | undefined,
    setTagHoveredIndex: (index: string | undefined) => void,
    options: { showTags: boolean; showTerms: boolean },
    filterText: string,
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
            <div data-testid={`schema-field-${newRecord.fieldPath}-${options.showTags ? 'tags' : 'terms'}`}>
                <TagTermGroup
                    uneditableTags={options.showTags ? newTags : null}
                    editableTags={options.showTags ? relevantEditableFieldInfo?.globalTags : null}
                    uneditableGlossaryTerms={options.showTerms ? newRecord.glossaryTerms : null}
                    editableGlossaryTerms={options.showTerms ? relevantEditableFieldInfo?.glossaryTerms : null}
                    canRemove
                    buttonProps={{ size: 'small' }}
                    canAddTag={tagHoveredIndex === newRecord.fieldPath && options.showTags}
                    canAddTerm={tagHoveredIndex === newRecord.fieldPath && options.showTerms}
                    onOpenModal={() => setTagHoveredIndex(undefined)}
                    entityUrn={urn}
                    entityType={EntityType.Dataset}
                    entitySubresource={newRecord.fieldPath}
                    highlightText={filterText}
                    refetch={refresh}
                />
            </div>
        );
    };
    return tagAndTermRender;
}
