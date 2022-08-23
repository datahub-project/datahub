import React from 'react';
import styled from 'styled-components';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';

const GroupAssetsWrapper = styled.div`
    height: calc(100vh - 114px);
`;

type Props = {
    urn: string;
};

export const GroupAssets = ({ urn }: Props) => {
    return (
        <GroupAssetsWrapper>
            <EmbeddedListSearchSection
                fixedFilter={{ field: 'owners', values: [urn] }}
                emptySearchQuery="*"
                placeholderText="Filter entities..."
            />
        </GroupAssetsWrapper>
    );
};
