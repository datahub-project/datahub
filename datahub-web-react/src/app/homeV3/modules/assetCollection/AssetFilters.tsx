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
                viewUrn={null}
            />
        </FiltersContainer>
    );
};

export default AssetFilters;
