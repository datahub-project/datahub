import { SearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import React from 'react';
import { useEntityData } from '../../entity/shared/EntityContext';
import { EntityType } from '../../../types.generated';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '../../search/utils/constants';

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
