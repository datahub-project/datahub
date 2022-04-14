import React from 'react';
import styled from 'styled-components';
import { EmbeddedListSearch } from '../shared/components/styled/search/EmbeddedListSearch';

const UserAssetsWrapper = styled.div`
    height: calc(100vh - 114px);
    overflow: auto;
`;

type Props = {
    urn: string;
};

export const UserAssets = ({ urn }: Props) => {
    return (
        <UserAssetsWrapper>
            <EmbeddedListSearch
                style={{ height: 'calc(100vh - 209px)' }}
                fixedFilter={{ field: 'owners', value: urn }}
                emptySearchQuery="*"
                placeholderText="Filter entities..."
            />
        </UserAssetsWrapper>
    );
};
