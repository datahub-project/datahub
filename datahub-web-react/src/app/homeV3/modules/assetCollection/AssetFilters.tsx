import React from 'react';
import styled from 'styled-components';

import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import Filters from '@app/searchV2/searchBarV2/components/Filters';
import { FacetMetadata } from '@src/types.generated';

const FiltersContainer = styled.div`
    margin: -8px;
`;

type Props = {
    searchQuery: string | undefined;
    appliedFilters?: FieldToAppliedFieldFiltersMap;
    updateFieldFilters?: AppliedFieldFilterUpdater;
    facets?: FacetMetadata[];
};

const AssetFilters = ({ searchQuery, appliedFilters, updateFieldFilters, facets }: Props) => {
    return (
        <FiltersContainer>
            <Filters
                query={searchQuery ?? '*'}
                appliedFilters={appliedFilters}
                updateFieldAppliedFilters={updateFieldFilters}
                viewUrn={null}
                facets={facets}
            />
        </FiltersContainer>
    );
};

export default AssetFilters;
