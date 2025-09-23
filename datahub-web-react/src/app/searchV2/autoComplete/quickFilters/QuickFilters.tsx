import React from 'react';
import styled from 'styled-components';

import QuickFilter from '@app/searchV2/autoComplete/quickFilters/QuickFilter';
import { useQuickFiltersContext } from '@providers/QuickFiltersContext';

const QuickFiltersWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    flex-flow: wrap;
`;

interface Props {
    searchQuery?: string;
    setIsDropdownVisible: React.Dispatch<React.SetStateAction<boolean>>;
}
export default function QuickFilters({ searchQuery, setIsDropdownVisible }: Props) {
    const { quickFilters } = useQuickFiltersContext();

    return (
        <QuickFiltersWrapper>
            {quickFilters?.map((quickFilter) => (
                <QuickFilter
                    key={quickFilter.value}
                    quickFilter={quickFilter}
                    searchQuery={searchQuery}
                    setIsDropdownVisible={setIsDropdownVisible}
                />
            ))}
        </QuickFiltersWrapper>
    );
}
