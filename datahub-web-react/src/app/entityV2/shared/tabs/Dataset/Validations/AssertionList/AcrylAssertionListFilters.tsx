import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { AcrylAssertionRecommendedFilters } from './AcrylAssertionRecommendedFilters';
import { AcrylAssertionListSearch } from './AcrylAssertionListSearch';
import { AcryAssertionTypeSelect } from './AcryAssertionTypeSelect';
import { AssertionListFilter, AssertionTable } from './types';
import { AcrylAssertionFilters } from './AcrylAssertionFilters';

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
    align-items: baseline;
    padding: 0px 10px;
`;

export const AcrylAssertionListFilters: React.FC<AcrylAssertionListFiltersProps> = ({
    filterOptions,
    setFilters,
    filter,
    allAssertionCount,
    filteredAssertions,
}) => {
    const [appliedFilters, setAppliedFilters] = useState<FilterItem[]>([]);
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
        /** Set Recommended Filters when there is value in type,status or others if not then set it as empty to clear the filter */
        const selectedRecommendedFilters = updatedFilters.reduce<Record<string, string[]>>(
            (acc, selectedfilter) => {
                acc[selectedfilter.category] = acc[selectedfilter.category] || [];
                acc[selectedfilter.category].push(selectedfilter.name);
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
                {/* ************Render Search Component ************************* */}
                <AcrylAssertionListSearch
                    searchText={filter.filterCriteria.searchText}
                    debouncedSetFilterText={handleSearchTextChange}
                    matchResultCount={filteredAssertions.assertions?.length || 0}
                    numRows={allAssertionCount}
                />

                {/* ************Render Filter Component ************************* */}
                <div>
                    <AcrylAssertionFilters
                        filterOptions={filterOptions?.filterGroupOptions || []}
                        selectedFilters={appliedFilters}
                        onFilterChange={handleFilterChange}
                    />
                </div>
                {/* ************Render Group By Component ************************* */}
                <div style={{ padding: '10px' }}>
                    <AcryAssertionTypeSelect
                        options={assertionTypeFilters}
                        selectedValue={selectedGroupBy}
                        onSelect={handleAssertionTypeChange}
                        placeholder="Group By"
                    />
                </div>
            </SearchFilterContainer>
            <div>
                {/* ************Render Recommended Filter Component ************************* */}
                <AcrylAssertionRecommendedFilters
                    filters={filterOptions?.recommendedFilters || []}
                    appliedFilters={appliedFilters}
                    onFilterChange={handleFilterChange}
                />
            </div>
        </>
    );
};
