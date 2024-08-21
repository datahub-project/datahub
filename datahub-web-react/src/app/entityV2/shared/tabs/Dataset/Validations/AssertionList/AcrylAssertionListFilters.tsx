import React, { Dispatch, SetStateAction, useEffect, useState } from 'react';
import { isEmpty } from 'lodash';
import AcrylAssertionRecommendedFilters from './AcrylAssertionRecommendedFilters';
import AcrylAssertionListSearch from './AcrylAssertionListSearch';
import AcryAssertionTypeSelect from './AcryAssertionTypeSelect';
import styled from 'styled-components';
import { AssertionListFilter, AssertionTable } from './types';
import { AcrylAssertionFilters } from './AcrylAssertionFilters';

interface FilterItem {
    name: string;
    category: string;
    count: number;
    displayName: string;
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
    allAssertionCount,
    filteredAssertions,
}: {
    filterOptions: any;
    setFilters: Dispatch<SetStateAction<AssertionListFilter>>;
    filter: AssertionListFilter;
    allAssertionCount: number;
    filteredAssertions: AssertionTable;
}) => {
    const [appliedFilters, setAppliedFilters] = useState<FilterItem[]>([]);
    const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | null>(null);
    const [selectedGroupBy, setSelectedGroupBy] = useState<string>('');

    const handleSearchTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const filterText = event.target.value;
        setFilters({ ...filter, filterCriteria: { ...filter.filterCriteria, searchText: filterText } });
    };

    const handleAssertionTypeChange = (value: string) => {
        setFilters({ ...filter, groupBy: value });
        setSelectedGroupBy(value);
    };

    const assertionTypeFilters: Array<{ label: string; value: string }> = [
        { label: 'Type', value: 'type' },
        { label: 'Status', value: 'status' },
    ];

    const handleFilterChange = (updatedFilters: FilterItem[]) => {
        let selectedRecommendedFilters: any = {};
        updatedFilters.forEach((filter: FilterItem) => {
            if (selectedRecommendedFilters[filter.category]) {
                selectedRecommendedFilters[filter.category].push(filter.name);
            } else {
                selectedRecommendedFilters[filter.category] = [filter.name];
            }
        });
        if (!selectedRecommendedFilters?.type) {
            selectedRecommendedFilters.type = [];
        }
        if (!selectedRecommendedFilters?.status) {
            selectedRecommendedFilters.status = [];
        }
        if (!selectedRecommendedFilters?.others) {
            selectedRecommendedFilters.others = [];
        }
        setFilters({ ...filter, filterCriteria: { ...filter.filterCriteria, ...selectedRecommendedFilters } });
        setAppliedFilters(updatedFilters);
    };

    useEffect(() => {
        const status = filter.filterCriteria?.status || [];
        const types = filter.filterCriteria?.type || [];
        const others = filter.filterCriteria?.others || [];
        const recommendedFilters = filterOptions?.recommendedFilters || [];
        let appliedRecommendedFilters = [];
        if (status.length > 0 || types.length > 0 || others.length > 0) {
            appliedRecommendedFilters = recommendedFilters.filter(
                (item) => status.includes(item.name) || types.includes(item.name) || others.includes(item.name),
            );
        }
        setSelectedGroupBy(filter.groupBy);
        setAppliedFilters(appliedRecommendedFilters);
    }, [filter, filterOptions]);

    return (
        <>
            <SearchFilterContainer>
                <AcrylAssertionListSearch
                    searchText={filter.filterCriteria.searchText}
                    debouncedSetFilterText={handleSearchTextChange}
                    matchResultCount={(filteredAssertions?.assertions || []).length}
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