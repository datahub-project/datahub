import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../shared/EntityContext';
import { EntityType } from '../../../types.generated';
import { EmbeddedListSearch } from '../shared/components/styled/search/EmbeddedListSearch';

const DomainEntitiesWrapper = styled.div`
    && .embeddedListSearchClass {
        height: calc(100vh - 290px);
        overflow-y: auto;
    }
`;

export const DomainEntitiesTab = () => {
    const { urn, entityType } = useEntityData();

    let fixedFilter;
    // Set a fixed filter corresponding to the current entity urn.
    if (entityType === EntityType.Domain) {
        fixedFilter = {
            field: 'domains',
            value: urn,
        };
    }

    return (
        <DomainEntitiesWrapper>
            <EmbeddedListSearch
                fixedFilter={fixedFilter}
                emptySearchQuery="*"
                placeholderText="Filter domain entities..."
            />
        </DomainEntitiesWrapper>
    );
};
