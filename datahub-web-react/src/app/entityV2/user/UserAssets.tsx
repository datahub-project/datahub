import React from 'react';
import styled from 'styled-components';
<<<<<<< HEAD

import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import useGetUserGroupUrns from '@app/entityV2/user/useGetUserGroupUrns';
import { UnionType } from '@app/search/utils/constants';
=======
import { UnionType } from '../../search/utils/constants';
import { EmbeddedListSearchSection } from '../shared/components/styled/search/EmbeddedListSearchSection';
import useGetUserGroupUrns from './useGetUserGroupUrns';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

const UserAssetsWrapper = styled.div`
    height: 100%;
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
