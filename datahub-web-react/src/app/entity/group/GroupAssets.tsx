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
