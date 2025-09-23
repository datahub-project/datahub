import React from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchSection } from '@app/entity/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';

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
