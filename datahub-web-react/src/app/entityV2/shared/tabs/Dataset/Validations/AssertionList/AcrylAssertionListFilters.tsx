import React, { useEffect, useState } from 'react';
import { isEmpty } from 'lodash';
import styled from 'styled-components';
import AcrylAssertionRecommendedFilters from './AcrylAssertionRecommendedFilters';
import AcrylAssertionListSearch from './AcrylAssertionListSearch';
import AcryAssertionTypeSelect from './AcryAssertionTypeSelect';
import { AcrylAssertionFilters } from './AcrylAssertionFilters';
import { AssertionListFilter, AssertionTable } from './types';

interface FilterItem {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface AcrylAssertionListFiltersProps {
    filterOptions: any;
    setFilters: React.Dispatch<React.SetStateAction<AssertionListFilter>>;
    filter: AssertionListFilter;
    allAssertionCount: number;
    filteredAssertions: AssertionTable;
}

const SearchFilterContainer = styled.div`
    display: flex;
    gap: 20px;
    align-items: center;
`;

export const AcrylAssertionListFilters: React.FC<AcrylAssertionListFiltersProps> = ({
    filterOptions,
    setFilters,
    filter,
    allAssertionCount,
    filteredAssertions,
}) => {
    const [appliedFilters, setAppliedFilters] = useState<FilterItem[]>([]);
    const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | null>(null);
    const [selectedGroupBy, setSelectedGroupBy] = useState<string>(filter.groupBy || '');

    const handleSearchTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const searchText = event.target.value;
        setFilters((prev) => ({
            ...prev,
            filterCriteria: { ...prev.filterCriteria, searchText },
        }));
    };

    const handleAssertionTypeChange = (value: string) => {
        setSelectedGroupBy(value);
        setFilters((prev) => ({ ...prev, groupBy: value }));
    };

    const assertionTypeFilters = [
        { label: 'Type', value: 'type' },
        { label: 'Status', value: 'status' },
    ];

    const handleFilterChange = (updatedFilters: FilterItem[]) => {
        const selectedRecommendedFilters = updatedFilters.reduce<Record<string, string[]>>(
            (acc, filter) => {
                acc[filter.category] = acc[filter.category] || [];
                acc[filter.category].push(filter.name);
                return acc;
            },
            { type: [], status: [], others: [] },
        );

        setFilters((prev) => ({
            ...prev,
            filterCriteria: { ...prev.filterCriteria, ...selectedRecommendedFilters },
        }));
        setAppliedFilters(updatedFilters);
    };

    useEffect(() => {
        const { status = [], type = [], others = [] } = filter.filterCriteria || {};
        const recommendedFilters = filterOptions?.recommendedFilters || [];
        const appliedRecommendedFilters = recommendedFilters.filter(
            (item) => status.includes(item.name) || type.includes(item.name) || others.includes(item.name),
        );

        setAppliedFilters(appliedRecommendedFilters);
        setSelectedGroupBy(filter.groupBy);
    }, [filter, filterOptions]);

    return (
        <>
            <SearchFilterContainer>
                <AcrylAssertionListSearch
                    searchText={filter.filterCriteria.searchText}
                    debouncedSetFilterText={handleSearchTextChange}
                    matchResultCount={filteredAssertions.assertions?.length || 0}
                    highlightedMatchIndex={highlightedMatchIndex}
                    setHighlightedMatchIndex={setHighlightedMatchIndex}
                    numRows={allAssertionCount}
                />
                <div>
                    <AcrylAssertionFilters
                        filterOptions={filterOptions?.filterGroupOptions || []}
                        selectedFilters={appliedFilters}
                        onFilterChange={handleFilterChange}
                    />
                </div>
                <div>
                    <AcryAssertionTypeSelect
                        options={assertionTypeFilters}
                        selectedValue={selectedGroupBy}
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
