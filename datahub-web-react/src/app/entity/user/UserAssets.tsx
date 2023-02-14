import React from 'react';
import styled from 'styled-components';
import { UnionType } from '../../search/utils/constants';
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
                fixedFilters={{
                    unionType: UnionType.AND,
                    filters: [{ field: 'owners', values: [urn] }],
                }}
                emptySearchQuery="*"
                placeholderText="Filter entities..."
            />
        </UserAssetsWrapper>
    );
};
