import React from 'react';

import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { useSchemaRefetch } from '@app/entity/shared/tabs/Dataset/Schema/SchemaContext';
import useExtractFieldGlossaryTermsInfo from '@app/entity/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo';
import useExtractFieldTagsInfo from '@app/entity/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo';
import TagTermGroup from '@app/shared/tags/TagTermGroup';

import { EditableSchemaMetadata, EntityType, GlobalTags, SchemaField } from '@types';

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
        const { uneditableTerms, editableTerms, proposedTerms } = extractFieldGlossaryTermsInfo(record);
        const { uneditableTags, editableTags, proposedTags } = extractFieldTagsInfo(record, tags);

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
