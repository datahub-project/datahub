/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/searchV2/utils/constants';

export function ApplicationEntitiesTab() {
    const { urn } = useEntityData();

    const fixedOrFilters = [
        {
            field: 'applications',
            values: [urn],
        },
    ];

    return (
        <SearchCardContext.Provider value={{ showRemovalFromList: true }}>
            <EmbeddedListSearchSection
                fixedFilters={{
                    unionType: UnionType.OR,
                    filters: fixedOrFilters,
                }}
                emptySearchQuery="*"
                placeholderText="Filter assets..."
                skipCache
                applyView
                shouldRefetch
            />
        </SearchCardContext.Provider>
    );
}
