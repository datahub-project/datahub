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

import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import Filters from '@app/searchV2/searchBarV2/components/Filters';

const FiltersContainer = styled.div`
    margin: -8px;
`;

type Props = {
    searchQuery: string | undefined;
    appliedFilters?: FieldToAppliedFieldFiltersMap;
    updateFieldFilters?: AppliedFieldFilterUpdater;
};

const AssetFilters = ({ searchQuery, appliedFilters, updateFieldFilters }: Props) => {
    return (
        <FiltersContainer>
            <Filters
                query={searchQuery ?? '*'}
                appliedFilters={appliedFilters}
                updateFieldAppliedFilters={updateFieldFilters}
            />
        </FiltersContainer>
    );
};

export default AssetFilters;
