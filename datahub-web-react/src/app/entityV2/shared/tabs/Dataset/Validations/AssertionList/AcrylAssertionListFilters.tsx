import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { AcrylListSearch } from '@src/app/entityV2/shared/components/ListSearch/AcrylListSearch';
import { AcrylAssertionRecommendedFilters } from './AcrylAssertionRecommendedFilters';
import { AcryAssertionTypeSelect } from './AcryAssertionTypeSelect';
import { AssertionListFilter, AssertionTable } from './types';
import { AcrylAssertionFilters } from './AcrylAssertionFilters';
import { ASSERTION_GROUP_BY_FILTER_OPTIONS, ASSERTION_DEFAULT_FILTERS } from './constant';

interface FilterItem {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface AcrylAssertionListFiltersProps {
    filterOptions: any;
    originalFilterOptions: any;
    setFilters: React.Dispatch<React.SetStateAction<AssertionListFilter>>;
    filter: AssertionListFilter;
    filteredAssertions: AssertionTable;
}

const SearchFilterContainer = styled.div`
    display: flex;
    padding: 0px 10px;
    margin-bottom: 8px;
    margin-top: 8px;
    gap: 12px;
    justify-content: space-between;
`;

const FiltersContainer = styled.div`
    display: flex;
`;

const StyledFilterContainer = styled.div`
    margin-right: 12px;
    button {
        box-shadow: none !important;
        height: 36px !important;
        font-size: 14px !important;
        border-radius: 8px !important;
        color: #5f6685;
    }
`;

export const AcrylAssertionListFilters: React.FC<AcrylAssertionListFiltersProps> = ({
    filterOptions,
    originalFilterOptions,
    setFilters,
    filter,
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
        /** Set Recommended Filters when there is value in type,status or source if not then set it as empty to clear the filter */
        const selectedRecommendedFilters = updatedFilters.reduce<Record<string, string[]>>(
            (acc, selectedfilter) => {
                acc[selectedfilter.category] = acc[selectedfilter.category] || [];
                acc[selectedfilter.category].push(selectedfilter.name);
                return acc;
            },
            { type: [], status: [], source: [], column: [] },
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
        const { status, type, source, column } = filter.filterCriteria || ASSERTION_DEFAULT_FILTERS.filterCriteria;
        const recommendedFilters = originalFilterOptions?.recommendedFilters || [];
        // just set recommended filters for status, type & Others as of right now
        const appliedRecommendedFilters = recommendedFilters.filter(
            (item) =>
                status.includes(item.name) ||
                type.includes(item.name) ||
                source.includes(item.name) ||
                column.includes(item.name),
        );
        setAppliedFilters(appliedRecommendedFilters);
        setSelectedGroupBy(filter.groupBy);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filter, filterOptions]);

    return (
        <>
            <SearchFilterContainer>
                {/* ************Render Search Component ************************* */}
                <AcrylListSearch
                    searchText={filter.filterCriteria.searchText}
                    debouncedSetFilterText={handleSearchTextChange}
                    matchResultCount={filteredAssertions.searchMatchesCount || 0}
                    numRows={filteredAssertions.totalCount || 0}
                    entityTypeName="assertion"
                />

                {/* ************Render Filter Component ************************* */}
                <FiltersContainer>
                    <StyledFilterContainer>
                        <AcrylAssertionFilters
                            filterOptions={originalFilterOptions?.filterGroupOptions || []}
                            selectedFilters={appliedFilters}
                            onFilterChange={handleFilterChange}
                        />
                    </StyledFilterContainer>
                    {/* ************Render Group By Component ************************* */}
                    <div>
                        <AcryAssertionTypeSelect
                            options={ASSERTION_GROUP_BY_FILTER_OPTIONS}
                            selectedValue={selectedGroupBy}
                            onSelect={handleAssertionTypeChange}
                        />
                    </div>
                </FiltersContainer>
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
