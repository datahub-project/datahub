import React from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchSection } from '@app/entity/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';
import useGetUserGroupUrns from '@src/app/entityV2/user/useGetUserGroupUrns';

const UserAssetsWrapper = styled.div`
    height: calc(100vh - 114px);
    overflow: auto;
`;

type Props = {
    urn: string;
};

export const UserAssets = ({ urn }: Props) => {
    const { groupUrns, data, loading } = useGetUserGroupUrns(urn);

    if (!data || loading) return null;

    return (
        <UserAssetsWrapper>
            <EmbeddedListSearchSection
                skipCache
                fixedFilters={{
                    unionType: UnionType.AND,
                    filters: [{ field: 'owners', values: [urn, ...groupUrns] }],
                }}
                emptySearchQuery="*"
                placeholderText="Filter entities..."
            />
        </UserAssetsWrapper>
    );
};
