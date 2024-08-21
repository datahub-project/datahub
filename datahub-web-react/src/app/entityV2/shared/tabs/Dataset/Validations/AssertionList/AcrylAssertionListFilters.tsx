import React, { Dispatch, SetStateAction, useEffect, useState } from 'react';
import { isEmpty } from 'lodash';
import AcrylAssertionRecommendedFilters from './AcrylAssertionRecommendedFilters';
import AcrylAssertionListSearch from './AcrylAssertionListSearch';
import AcryAssertionTypeSelect from './AcryAssertionTypeSelect';
import styled from 'styled-components';
import { AssertionListFilter } from './types';

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

export const AcrylAssertionListFilters = ({
    filterOptions,
    setFilters,
    filter,
}: {
    filterOptions: any;
    setFilters: Dispatch<SetStateAction<AssertionListFilter>>;
    filter: AssertionListFilter;
}) => {
    const [appliedFilters, setAppliedFilters] = useState<FilterItem[]>([]);
    const [assertionFilter, setAssertionFilter] = useState<string>('');
    const [matches, setMatches] = useState<string[]>([]);
    const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | null>(null);
    const [selectedAssertionType, setSelectedAssertionType] = useState<string>(''); // Updated to allow null
    const handleFilterTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const filterText = event.target.value;
        setAssertionFilter(filterText);

        const newMatches = [];
        setMatches(newMatches);
        setHighlightedMatchIndex(null);
    };

    const handleAssertionTypeChange = (value: string) => {
        setFilters({ ...filter, groupBy: value });

        setSelectedAssertionType(value);
    };

    useEffect(() => {
        console.log('filterOptions>>>', filterOptions);
    }, [filterOptions]);

    const numRows = 100;

    const assertionTypeFilters: Array<{ label: string; value: string }> = [
        { label: 'Type', value: 'type' },
        { label: 'Status', value: 'status' },
    ];

    const handleFilterChange = (updatedFilters: FilterItem[]) => {
        let selectedRecommendedFilters = {};
        updatedFilters.forEach((filter: FilterItem) => {
            if (selectedRecommendedFilters[filter.category]) {
                selectedRecommendedFilters[filter.category].push(filter.name);
            } else {
                selectedRecommendedFilters[filter.category] = [filter.name];
            }
        });
        if (isEmpty(selectedRecommendedFilters)) {
            selectedRecommendedFilters = {
                type: [],
                status: [],
            };
        }
        setFilters({ ...filter, filterCriteria: { ...filter.filterCriteria, ...selectedRecommendedFilters } });
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
                        selectedValue={selectedAssertionType}
                        onSelect={handleAssertionTypeChange}
                        placeholder="Group By"
                    />
                </div>
            </SearchFilterContainer>
            <div>
                <AcrylAssertionRecommendedFilters
                    filters={filterOptions?.recommendedFilters || []}
                    appliedFilters={appliedFilters}
                    onFilterChange={handleFilterChange}
                />
            </div>
        </>
    );
};
