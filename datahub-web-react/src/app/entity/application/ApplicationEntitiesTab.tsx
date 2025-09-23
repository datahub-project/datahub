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
            />
        </SearchCardContext.Provider>
    );
}
