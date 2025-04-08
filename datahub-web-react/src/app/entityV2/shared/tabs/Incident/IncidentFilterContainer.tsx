import React, { useMemo } from 'react';

import { FilterSelect } from '@app/entityV2/shared/FilterSelect';
import { GroupBySelect } from '@app/entityV2/shared/GroupBySelect';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import {
    INCIDENT_DEFAULT_FILTERS,
    INCIDENT_GROUP_BY_FILTER_OPTIONS,
} from '@app/entityV2/shared/tabs/Incident/constant';
import {
    FiltersContainer,
    SearchFilterContainer,
    StyledFilterContainer,
} from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { IncidentTable } from '@app/entityV2/shared/tabs/Incident/types';

interface FilterItem {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface IncidentAssigneeAvatarStack {
    filteredIncidents: IncidentTable;
    originalFilterOptions: any;
    handleFilterChange: (filter: any) => void;
    selectedFilters: any;
}

export const IncidentFilterContainer: React.FC<IncidentAssigneeAvatarStack> = ({
    filteredIncidents,
    originalFilterOptions,
    handleFilterChange,
    selectedFilters,
}) => {
    const handleSearchTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const searchText = event.target.value;
        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, searchText },
        });
    };

    const handleIncidentGroupByChange = (value: string) => {
        handleFilterChange({ ...selectedFilters, groupBy: value });
    };

    const handleFilterOptionChange = (updatedFilters: FilterItem[]) => {
        /** Set Recommended Filters when there is value in type,stage or priority if not then set it as empty to clear the filter */
        const selectedRecommendedFilters = updatedFilters.reduce<Record<string, string[]>>(
            (acc, selectedfilter) => {
                acc[selectedfilter.category] = acc[selectedfilter.category] || [];
                acc[selectedfilter.category].push(selectedfilter.name);
                return acc;
            },
            { type: [], stage: [], priority: [], state: [] },
        );

        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, ...selectedRecommendedFilters },
        });
    };

    const initialSelectedOptions = useMemo(() => {
        const recommendedFilters = originalFilterOptions?.recommendedFilters || [];
        const { stage, type, priority, state } =
            selectedFilters.filterCriteria || INCIDENT_DEFAULT_FILTERS.filterCriteria;

        const appliedRecommendedFilters = recommendedFilters.filter(
            (item) =>
                (state.includes(item.name) && item.category === 'state') ||
                (stage.includes(item.name) && item.category === 'stage') ||
                (type.includes(item.name) && item.category === 'type') ||
                (priority.includes(item.name) && item.category === 'priority'),
        );
        return appliedRecommendedFilters?.map((filter) => ({
            value: filter.name,
            label: filter.displayName,
            parentValue: filter.category,
        }));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedFilters]);

    return (
        <SearchFilterContainer>
            {/* ************Render Search Component ************************* */}
            <InlineListSearch
                searchText={selectedFilters.filterCriteria.searchText}
                debouncedSetFilterText={handleSearchTextChange}
                matchResultCount={filteredIncidents.searchMatchesCount || 0}
                numRows={filteredIncidents.totalCount || 0}
                entityTypeName="incident"
            />

            {/* ************Render Filter Component ************************* */}
            <FiltersContainer>
                <StyledFilterContainer>
                    <FilterSelect
                        filterOptions={originalFilterOptions?.filterGroupOptions || []}
                        onFilterChange={handleFilterOptionChange}
                        initialSelectedOptions={initialSelectedOptions}
                    />
                </StyledFilterContainer>
                {/* ************Render Group By Component ************************* */}
                <div>
                    <GroupBySelect
                        options={INCIDENT_GROUP_BY_FILTER_OPTIONS}
                        selectedValue={selectedFilters.groupBy}
                        onSelect={handleIncidentGroupByChange}
                    />
                </div>
            </FiltersContainer>
        </SearchFilterContainer>
    );
};
