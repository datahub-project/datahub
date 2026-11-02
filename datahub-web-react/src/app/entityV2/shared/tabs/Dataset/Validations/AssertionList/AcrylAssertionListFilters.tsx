import React, { useMemo } from 'react';
import styled from 'styled-components';

import { FilterSelect } from '@app/entityV2/shared/FilterSelect';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import { AcrylAssertionRecommendedFilters } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionRecommendedFilters';
import { ASSERTION_DEFAULT_FILTERS } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import { useSetFilterFromURLParams } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/hooks';
import { AssertionListFilter } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { extractFilterOptionsFromFacets } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import { Assertion, FacetMetadata } from '@src/types.generated';

interface FilterItem {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface AcrylAssertionListFiltersProps {
    setSelectedFilters: React.Dispatch<React.SetStateAction<AssertionListFilter>>;
    filteredAssertions: Assertion[];
    selectedFilters: AssertionListFilter;
    handleFilterChange: (filter: AssertionListFilter) => void;
    totalAssertionCount: number;
    facets?: FacetMetadata[];
}

const ASSERTION_ENTITY_TYPE = 'assertion';

const SearchFilterContainer = styled.div`
    display: flex;
    margin-bottom: 8px;
    margin-top: 8px;
    gap: 12px;
    justify-content: space-between;
`;

const FiltersContainer = styled.div`
    display: flex;
`;

const StyledFilterContainer = styled.div`
    button {
        box-shadow: none !important;
        height: 36px !important;
        font-size: 14px !important;
        border-radius: 8px !important;
        color: ${(props) => props.theme.colors.textSecondary};
    }
`;

export const AcrylAssertionListFilters: React.FC<AcrylAssertionListFiltersProps> = ({
    filteredAssertions,
    handleFilterChange,
    selectedFilters,
    setSelectedFilters,
    totalAssertionCount,
    facets,
}) => {
    const filterOptions = extractFilterOptionsFromFacets(filteredAssertions, facets);

    const handleSearchTextChange = (searchText: string) => {
        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, searchText },
        });
    };

    const handleFilterOptionChange = (updatedFilters: FilterItem[]) => {
        const selectedRecommendedFilters = updatedFilters.reduce<Record<string, string[]>>(
            (acc, selectedfilter) => {
                acc[selectedfilter.category] = acc[selectedfilter.category] || [];
                acc[selectedfilter.category].push(selectedfilter.name);
                return acc;
            },
            { type: [], status: [], source: [], column: [], tags: [], owners: [] },
        );

        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, ...selectedRecommendedFilters },
        });
    };

    /**
     * This hook is for setting applied filter when we are getting it from selected Filter state
     */
    const appliedRecommendedFilters = useMemo(() => {
        const { status, type, source, column, tags, owners } =
            selectedFilters.filterCriteria || ASSERTION_DEFAULT_FILTERS.filterCriteria;
        const recommendedFilters = filterOptions?.recommendedFilters || [];
        const selectedNames = new Set<string>([...status, ...type, ...source, ...column, ...tags, ...owners]);
        return recommendedFilters.filter((item) => selectedNames.has(item.name));
    }, [filterOptions?.recommendedFilters, selectedFilters.filterCriteria]);

    const initialSelectedOptions = useMemo(
        () =>
            appliedRecommendedFilters.map((filter) => ({
                value: filter.name,
                label: filter.displayName,
                parentValue: filter.category,
            })),
        [appliedRecommendedFilters],
    );

    // set the filter if there is any url filter object presents
    useSetFilterFromURLParams(selectedFilters, setSelectedFilters);

    return (
        <>
            <SearchFilterContainer>
                {/* ************Render Search Component ************************* */}
                <InlineListSearch
                    searchText={selectedFilters.filterCriteria?.searchText}
                    debouncedSetFilterText={handleSearchTextChange}
                    matchResultCount={filteredAssertions?.length}
                    numRows={totalAssertionCount}
                    entityTypeName={ASSERTION_ENTITY_TYPE}
                    options={{ hideMatchCountText: true }}
                />

                {/* ************Render Filter Component ************************* */}
                <FiltersContainer>
                    <StyledFilterContainer>
                        <FilterSelect
                            filterOptions={filterOptions?.filterGroupOptions || []}
                            onFilterChange={handleFilterOptionChange}
                            initialSelectedOptions={initialSelectedOptions}
                        />
                    </StyledFilterContainer>
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
