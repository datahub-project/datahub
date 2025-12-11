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
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import TagTermGroup from '@app/shared/tags/TagTermGroup';

import { EditableSchemaMetadata, EntityType, GlobalTags, SchemaField } from '@types';

export default function useTagsAndTermsRendererFeatureTable(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    tagHoveredIndex: string | undefined,
    setTagHoveredIndex: (index: string | undefined) => void,
    options: { showTags: boolean; showTerms: boolean },
) {
    const { urn } = useEntityData();
    const refetch = useRefetch();

    const tagAndTermRender = (tags: GlobalTags, record: SchemaField, rowIndex: number | undefined) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        return (
            <div data-testid={`schema-field-${record.fieldPath}-${options.showTags ? 'tags' : 'terms'}`}>
                <TagTermGroup
                    uneditableTags={options.showTags ? tags : null}
                    editableTags={options.showTags ? relevantEditableFieldInfo?.globalTags : null}
                    uneditableGlossaryTerms={options.showTerms ? record.glossaryTerms : null}
                    editableGlossaryTerms={options.showTerms ? relevantEditableFieldInfo?.glossaryTerms : null}
                    canRemove
                    buttonProps={{ size: 'small' }}
                    canAddTag={tagHoveredIndex === `${record.fieldPath}-${rowIndex}` && options.showTags}
                    canAddTerm={tagHoveredIndex === `${record.fieldPath}-${rowIndex}` && options.showTerms}
                    onOpenModal={() => setTagHoveredIndex(undefined)}
                    entityUrn={urn}
                    entityType={EntityType.Dataset}
                    entitySubresource={record.fieldPath}
                    refetch={refetch}
                />
            </div>
        );
    };
    return tagAndTermRender;
}
