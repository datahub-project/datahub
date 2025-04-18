import React from 'react';
import styled from 'styled-components';
import { UnionType } from '../../search/utils/constants';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';

const GroupAssetsWrapper = styled.div`
    height: 100%;
`;

type Props = {
    urn: string;
};

export const GroupAssets = ({ urn }: Props) => {
    return (
        <GroupAssetsWrapper>
            <EmbeddedListSearchSection
                skipCache
                fixedFilters={{
                    unionType: UnionType.AND,
                    filters: [{ field: 'owners', values: [urn] }],
                }}
                emptySearchQuery="*"
                placeholderText="Filter entities..."
            />
        </GroupAssetsWrapper>
    );
};
