import React from 'react';
import styled from 'styled-components';
import { EmbeddedListSearch } from '../shared/components/styled/search/EmbeddedListSearch';

const GroupAssetsWrapper = styled.div`
    height: calc(100vh - 114px);
    overflow: auto;
`;

type Props = {
    urn: string;
};

export const GroupAssets = ({ urn }: Props) => {
    return (
        <GroupAssetsWrapper>
            <EmbeddedListSearch
                fixedFilter={{ field: 'owners', value: urn }}
                emptySearchQuery="*"
                placeholderText="Filter entities..."
            />
        </GroupAssetsWrapper>
    );
};
