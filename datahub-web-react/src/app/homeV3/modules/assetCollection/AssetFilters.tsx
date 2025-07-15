import React from 'react';
import styled from 'styled-components';

import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import Filters from '@app/searchV2/searchBarV2/components/Filters';

const FiltersContainer = styled.div`
    margin: -8px;
`;

type Props = {
    searchQuery: string | undefined;
    appliedFilters: FieldToAppliedFieldFiltersMap;
    setAppliedFilters: React.Dispatch<React.SetStateAction<FieldToAppliedFieldFiltersMap>>;
};

const AssetFilters = ({ searchQuery, appliedFilters, setAppliedFilters }: Props) => {
    const updateFieldAppliedFilters: AppliedFieldFilterUpdater = (field, value) => {
        setAppliedFilters((prev) => {
            const next = new Map(prev);
            next.set(field, value);
            return next;
        });
    };

    return (
        <FiltersContainer>
            <Filters
                query={searchQuery ?? '*'}
                appliedFilters={appliedFilters}
                updateFieldAppliedFilters={updateFieldAppliedFilters}
            />
        </FiltersContainer>
    );
};

export default AssetFilters;
