import React from 'react';
import styled from 'styled-components';
import { EmbeddedListSearch } from '../shared/components/styled/search/EmbeddedListSearch';
import { useEntityData } from '../shared/EntityContext';

const UserAssetsWrapper = styled.div`
    height: calc(100vh - 114px);
    overflow: auto;
`;
export const UserAssets = () => {
    const { urn } = useEntityData();

    return (
        <UserAssetsWrapper>
            <EmbeddedListSearch
                fixedFilter={{ field: 'owners', value: urn }}
                emptySearchQuery="*"
                placeholderText="Filter domain entities..."
            />
        </UserAssetsWrapper>
    );
};
