import React from 'react';
import styled from 'styled-components';
import { useQuickFiltersContext } from '../../../../providers/QuickFiltersContext';
import QuickFilter from './QuickFilter';

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
