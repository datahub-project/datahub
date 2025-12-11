/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import TagTermGroup from '@app/shared/tags/TagTermGroup';

import { BusinessAttribute, EntityType, GlobalTags } from '@types';

export default function useTagsAndTermsRenderer(
    tagHoveredUrn: string | undefined,
    setTagHoveredUrn: (index: string | undefined) => void,
    options: { showTags: boolean; showTerms: boolean },
    filterText: string,
    businessAttributeRefetch: () => Promise<any>,
) {
    const urn = tagHoveredUrn;

    const refresh: any = () => {
        businessAttributeRefetch?.();
    };

    const tagAndTermRender = (tags: GlobalTags, record: BusinessAttribute) => {
        return (
            <div data-testid={`schema-field-${record.properties?.name}-${options.showTags ? 'tags' : 'terms'}`}>
                <TagTermGroup
                    editableTags={options.showTags ? tags : null}
                    editableGlossaryTerms={options.showTerms ? record.properties?.glossaryTerms : null}
                    canRemove
                    buttonProps={{ size: 'small' }}
                    canAddTag={tagHoveredUrn === record.urn && options.showTags}
                    canAddTerm={tagHoveredUrn === record.urn && options.showTerms}
                    onOpenModal={() => setTagHoveredUrn(undefined)}
                    entityUrn={urn}
                    entityType={EntityType.BusinessAttribute}
                    highlightText={filterText}
                    refetch={refresh}
                />
            </div>
        );
    };
    return tagAndTermRender;
}
