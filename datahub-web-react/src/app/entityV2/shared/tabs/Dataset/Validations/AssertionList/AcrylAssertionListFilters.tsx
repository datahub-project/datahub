import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { AcrylAssertionRecommendedFilters } from './AcrylAssertionRecommendedFilters';
import { AcrylAssertionListSearch } from './AcrylAssertionListSearch';
import { AcryAssertionTypeSelect } from './AcryAssertionTypeSelect';
import { AssertionListFilter, AssertionTable } from './types';
import { AcrylAssertionFilters } from './AcrylAssertionFilters';
import { ASSERTION_GROUP_BY_FILTER_OPTIONS, DEFAULT_FILTERS } from './constant';

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
    padding: 0px 10px;
    margin-bottom: 8px;
    margin-top: 8px;
`;

export const AcrylAssertionListFilters: React.FC<AcrylAssertionListFiltersProps> = ({
    filterOptions,
    setFilters,
    filter,
    allAssertionCount,
    filteredAssertions,
}) => {
    const [appliedFilters, setAppliedFilters] = useState<FilterItem[]>([]);
    const [selectedGroupBy, setSelectedGroupBy] = useState<string | undefined>(filter.groupBy || undefined);

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

    /**
     * This hook is for setting applied filter when we are getting it from selected Filter state
     */
    useEffect(() => {
        const { status, type, others } = filter.filterCriteria || DEFAULT_FILTERS.filterCriteria;
        const recommendedFilters = filterOptions?.recommendedFilters || [];
        // just set recommended filters for status, type & Others as of right now
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
                <div style={{ marginLeft: 8 }}>
                    <AcryAssertionTypeSelect
                        options={ASSERTION_GROUP_BY_FILTER_OPTIONS}
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
