import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EmbeddedListSearchSection } from '@app/entity/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';

export const ContainerEntitiesTab = () => {
    const { urn } = useEntityData();

    const fixedFilter = {
        field: 'container',
        values: [urn],
    };

    return (
        <EmbeddedListSearchSection
            fixedFilters={{
                unionType: UnionType.AND,
                filters: [fixedFilter],
            }}
            emptySearchQuery="*"
            placeholderText="Filter container entities..."
            applyView
        />
    );
};
