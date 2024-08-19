import React, { useState } from 'react';
import AcrylAssertionRecommendedFilters from './AcrylAssertionRecommendedFilters';
import AcrylAssertionListSearch from './AcrylAssertionListSearch';
import AcryAssertionTypeSelect from './AcryAssertionTypeSelect';
import styled from 'styled-components';

interface FilterItem {
    name: string;
    category: string;
    count: number;
}

const SearchFilterContainer = styled.div`
    display: flex;
    gap: 20px;
    align-items: center;
`;

export const AcrylAssertionListFilters = () => {
    const [appliedFilters, setAppliedFilters] = useState<FilterItem[]>([]);
    const [assertionFilter, setAssertionFilter] = useState<string>('');
    const [matches, setMatches] = useState<string[]>([]);
    const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | null>(null);
    const [selectedAssertionType, setSelectedAssertionType] = useState<string | null>(null); // Updated to allow null

    const handleFilterTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const filterText = event.target.value;
        setAssertionFilter(filterText);

        const newMatches = [];
        setMatches(newMatches);
        setHighlightedMatchIndex(null);
    };

    const handleAssertionTypeChange = (value: string | null) => {
        setSelectedAssertionType(value); 
    };

    const numRows = 100;

    const filters: FilterItem[] = [
        { name: 'Freshness', category: 'type', count: 10 },
        { name: 'Volume', category: 'type', count: 5 },
        { name: 'Column', category: 'type', count: 15 },
        { name: 'Custom', category: 'type', count: 10 },
        { name: 'Schema', category: 'type', count: 5 },
        { name: 'Failing', category: 'status', count: 15 },
        { name: 'Passing', category: 'status', count: 10 },
        { name: 'Native', category: 'other', count: 5 },
        { name: 'Smart Assertion', category: 'other', count: 15 },
        { name: 'External', category: 'other', count: 15 },
    ];

    const assertionTypeFilters: Array<{ label: string; value: string }> = [
        { label: 'Type', value: 'type' },
        { label: 'Status', value: 'status' },
    ];

    const handleFilterChange = (updatedFilters: FilterItem[]) => {
        setAppliedFilters(updatedFilters);
    };

    return (
        <>
            <SearchFilterContainer>
                <AcrylAssertionListSearch
                    assertionFilter={assertionFilter}
                    debouncedSetFilterText={handleFilterTextChange}
                    matches={matches}
                    highlightedMatchIndex={highlightedMatchIndex}
                    setHighlightedMatchIndex={setHighlightedMatchIndex}
                    numRows={numRows}
                />
                <div>
                    <AcryAssertionTypeSelect
                        options={assertionTypeFilters}
                        selectedValue={selectedAssertionType || ''}
                        onSelect={handleAssertionTypeChange}
                        placeholder="Group By"
                    />
                </div>
            </SearchFilterContainer>
            <div>
                <AcrylAssertionRecommendedFilters
                    filters={filters}
                    appliedFilters={appliedFilters}
                    onFilterChange={handleFilterChange}
                />
            </div>
        </>
    );
};