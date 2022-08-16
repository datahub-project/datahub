import React from 'react';
import styled from 'styled-components';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';

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
            <EmbeddedListSearchSection
                fixedFilter={{ field: 'owners', value: urn }}
                emptySearchQuery="*"
                placeholderText="Filter entities..."
            />
        </UserAssetsWrapper>
    );
};
