/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as React from 'react';

import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

export default function BusinessAttributeRelatedEntity() {
    const { entityData } = useEntityData();

    const entityUrn = entityData?.urn;

    const fixedOrFilters =
        (entityUrn && [
            {
                field: 'businessAttribute',
                values: [entityUrn],
            },
        ]) ||
        [];

    entityData?.isAChildren?.relationships?.forEach((businessAttribute) => {
        const childUrn = businessAttribute.entity?.urn;

        if (childUrn) {
            fixedOrFilters.push({
                field: 'businessAttributes',
                values: [childUrn],
            });
        }
    });

    return (
        <EmbeddedListSearchSection
            fixedFilters={{
                unionType: UnionType.OR,
                filters: fixedOrFilters,
            }}
            emptySearchQuery="*"
            placeholderText="Filter entities..."
            skipCache
            applyView
        />
    );
}
