/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import useGetUserGroupUrns from '@app/entityV2/user/useGetUserGroupUrns';
import { UnionType } from '@app/search/utils/constants';

const UserAssetsWrapper = styled.div`
    height: 100%;
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
