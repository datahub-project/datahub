import React from 'react';
import { useEntityData } from '../../entity/shared/EntityContext';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '../../search/utils/constants';

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
