/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { pathMatchesNewPath } from '@app/entity/dataset/profile/schema/utils/utils';
import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { useSchemaRefetch } from '@app/entity/shared/tabs/Dataset/Schema/SchemaContext';
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
                uneditableTags={options.showTags ? { tags: [...(tags?.tags || []), ...businessAttributeTags] } : null}
                editableTags={options.showTags ? relevantEditableFieldInfo?.globalTags : null}
                uneditableGlossaryTerms={
                    options.showTerms
                        ? { terms: [...(record?.glossaryTerms?.terms || []), ...businessAttributeTerms] }
                        : null
                }
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
