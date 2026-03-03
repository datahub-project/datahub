import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';

import { EntityType } from '@types';

export const DomainEntitiesTab = () => {
    const { urn, entityType } = useEntityData();

    let fixedFilter;
    // Set a fixed filter corresponding to the current entity urn.
    if (entityType === EntityType.Domain) {
        fixedFilter = {
            field: 'domains',
            values: [urn],
        };
    }

    const excludeFromFilter = { field: '_entityType', values: ['DATA_PRODUCT'], value: 'DATA_PRODUCT', negated: true };

    return (
        <SearchCardContext.Provider value={{ showRemovalFromList: true }}>
            <EmbeddedListSearchSection
                fixedFilters={{
                    unionType: UnionType.AND,
                    filters: [excludeFromFilter, fixedFilter],
                }}
                emptySearchQuery="*"
                placeholderText="Filter domain entities..."
                skipCache
                applyView
            />
        </SearchCardContext.Provider>
    );
};
