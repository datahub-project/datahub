import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { FilterSelect } from '@src/app/entityV2/shared/FilterSelect';
import { GroupBySelect } from '@src/app/entityV2/shared/GroupBySelect';
import { InlineListSearch } from '@src/app/entityV2/shared/components/search/InlineListSearch';
import { AcrylAssertionRecommendedFilters } from './AcrylAssertionRecommendedFilters';
import { AssertionListFilter, AssertionTable } from './types';
import { ASSERTION_GROUP_BY_FILTER_OPTIONS, ASSERTION_DEFAULT_FILTERS, ASSERTION_FILTER_TYPES } from './constant';
import { useSetFilterFromURLParams } from './hooks';

interface FilterItem {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface AcrylAssertionListFiltersProps {
    filterOptions: any;
    originalFilterOptions: any;
    setSelectedFilters: React.Dispatch<React.SetStateAction<AssertionListFilter>>;
    filteredAssertions: AssertionTable;
    selectedFilters: any;
    handleFilterChange: (filter: any) => void;
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
    filteredAssertions,
    handleFilterChange,
    selectedFilters,
    setSelectedFilters,
}) => {
    const [appliedRecommendedFilters, setAppliedRecommendedFilters] = useState([]);

    const handleSearchTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const searchText = event.target.value;
        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, searchText },
        });
    };

    const handleAssertionTypeChange = (value: string) => {
        handleFilterChange({ ...selectedFilters, groupBy: value });
    };

    const handleFilterOptionChange = (updatedFilters: FilterItem[]) => {
        /** Set Recommended Filters when there is value in type,status or source if not then set it as empty to clear the filter */
        const selectedRecommendedFilters = updatedFilters.reduce<Record<string, string[]>>(
            (acc, selectedfilter) => {
                acc[selectedfilter.category] = acc[selectedfilter.category] || [];
                acc[selectedfilter.category].push(selectedfilter.name);
                return acc;
            },
            { type: [], status: [], source: [], column: [] },
        );

        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, ...selectedRecommendedFilters },
        });
    };

    /**
     * This hook is for setting applied filter when we are getting it from selected Filter state
     */
    const initialSelectedOptions = useMemo(() => {
        const { status, type, source, column } =
            selectedFilters.filterCriteria || ASSERTION_DEFAULT_FILTERS.filterCriteria;
        const recommendedFilters = originalFilterOptions?.recommendedFilters || [];
        // just set recommended filters for status, type & Others as of right now
        const selectedRecommendedFilters = recommendedFilters.filter(
            (item) =>
                status.includes(item.name) ||
                type.includes(item.name) ||
                source.includes(item.name) ||
                column.includes(item.name),
        );
        setAppliedRecommendedFilters(selectedRecommendedFilters);
        return selectedRecommendedFilters?.map((filter) => ({
            value: filter.name,
            label: filter.displayName,
            parentValue: filter.category,
        }));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedFilters]);

    // set the filter if there is any url filter object presents
    useSetFilterFromURLParams(selectedFilters, setSelectedFilters);

    return (
        <>
            <SearchFilterContainer>
                {/* ************Render Search Component ************************* */}
                <InlineListSearch
                    searchText={selectedFilters.filterCriteria?.searchText}
                    debouncedSetFilterText={handleSearchTextChange}
                    matchResultCount={filteredAssertions.searchMatchesCount || 0}
                    numRows={filteredAssertions.totalCount || 0}
                    entityTypeName="assertion"
                />

                {/* ************Render Filter Component ************************* */}
                <FiltersContainer>
                    <StyledFilterContainer>
                        <FilterSelect
                            filterOptions={originalFilterOptions?.filterGroupOptions || []}
                            onFilterChange={handleFilterOptionChange}
                            excludedCategories={[ASSERTION_FILTER_TYPES.TAG]}
                            initialSelectedOptions={initialSelectedOptions}
                        />
                    </StyledFilterContainer>
                    {/* ************Render Group By Component ************************* */}
                    <div>
                        <GroupBySelect
                            options={ASSERTION_GROUP_BY_FILTER_OPTIONS}
                            selectedValue={selectedFilters.groupBy}
                            onSelect={handleAssertionTypeChange}
                            width={50}
                        />
                    </div>
                </FiltersContainer>
            </SearchFilterContainer>
            <div>
                {/* ************Render Recommended Filter Component ************************* */}
                <AcrylAssertionRecommendedFilters
                    filters={filterOptions?.recommendedFilters || []}
                    appliedFilters={appliedRecommendedFilters}
                    onFilterChange={handleFilterOptionChange}
                />
            </div>
        </>
    );
};
