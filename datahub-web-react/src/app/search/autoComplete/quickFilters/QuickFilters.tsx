import React from 'react';
import styled from 'styled-components';

import QuickFilter from '@app/search/autoComplete/quickFilters/QuickFilter';
import { useQuickFiltersContext } from '@providers/QuickFiltersContext';

const QuickFiltersWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    flex-flow: wrap;
`;

export default function QuickFilters() {
    const { quickFilters } = useQuickFiltersContext();

    return (
        <QuickFiltersWrapper>
            {quickFilters?.map((quickFilter) => <QuickFilter key={quickFilter.value} quickFilter={quickFilter} />)}
        </QuickFiltersWrapper>
    );
}
