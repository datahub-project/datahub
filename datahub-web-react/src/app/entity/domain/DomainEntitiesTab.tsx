import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EmbeddedListSearchSection } from '@app/entity/shared/components/styled/search/EmbeddedListSearchSection';
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

    return (
        <EmbeddedListSearchSection
            fixedFilters={{
                unionType: UnionType.AND,
                filters: [fixedFilter],
            }}
            emptySearchQuery="*"
            placeholderText="Filter domain entities..."
            skipCache
            applyView
        />
    );
};
