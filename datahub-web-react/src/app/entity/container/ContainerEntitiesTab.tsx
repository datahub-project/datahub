import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../shared/EntityContext';
import { EmbeddedListSearch } from '../shared/components/styled/search/EmbeddedListSearch';

const ContainerEntitiesWrapper = styled.div`
    && .embeddedListSearchClass {
        height: calc(100vh - 292px);
        overflow-y: auto;
    }
`;

export const ContainerEntitiesTab = () => {
    const { urn } = useEntityData();

    const fixedFilter = {
        field: 'container',
        value: urn,
    };

    return (
        <ContainerEntitiesWrapper>
            <EmbeddedListSearch
                fixedFilter={fixedFilter}
                emptySearchQuery="*"
                placeholderText="Filter container entities..."
            />
        </ContainerEntitiesWrapper>
    );
};
