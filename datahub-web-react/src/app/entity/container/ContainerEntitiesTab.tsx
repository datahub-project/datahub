import React from 'react';
import { useEntityData } from '../shared/EntityContext';
import { EmbeddedListSearch } from '../shared/components/styled/search/EmbeddedListSearch';

export const ContainerEntitiesTab = () => {
    const { urn } = useEntityData();

    const fixedFilter = {
        field: 'container',
        value: urn,
    };

    return (
        <>
            <EmbeddedListSearch
                style={{ height: 'calc(100vh - 292px)' }}
                fixedFilter={fixedFilter}
                emptySearchQuery="*"
                placeholderText="Filter container entities..."
            />
        </>
    );
};
