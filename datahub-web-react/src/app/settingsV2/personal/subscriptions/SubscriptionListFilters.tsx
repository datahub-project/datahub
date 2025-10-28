import { SearchBar, SimpleSelect } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { SubscriptionFilterOptions, SubscriptionListFilter } from '@app/settingsV2/personal/subscriptions/types';
import { extractFilterOptionListFromSubscriptions } from '@app/settingsV2/personal/subscriptions/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpUser, DataHubSubscription, EntityType } from '@types';

interface SubscriptionListFiltersProps {
    viewer: CorpUser;
    selectedFilters: SubscriptionListFilter;
    handleFilterChange: (filter: SubscriptionListFilter) => void;
    subscriptions: DataHubSubscription[];
    ownedAndMemberGroupUrns: string[];
}

const SearchFilterContainer = styled.div`
    display: flex;
    gap: 12px;
    justify-content: space-between;
`;

const FilterOptionsWrapper = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

export const SubscriptionListFilters: React.FC<SubscriptionListFiltersProps> = ({
    viewer,
    subscriptions,
    handleFilterChange,
    selectedFilters,
    ownedAndMemberGroupUrns,
}) => {
    const entityRegistry = useEntityRegistryV2();
    const [initialFilterOptions, setInitialFilterOptions] = useState<SubscriptionFilterOptions | null>(null);

    // Store the initial filter options when subscriptions first load (broadest set of results)
    useEffect(() => {
        if (subscriptions.length > 0 && !initialFilterOptions) {
            const filterOptions = extractFilterOptionListFromSubscriptions(
                subscriptions,
                entityRegistry,
                viewer,
                ownedAndMemberGroupUrns,
            );
            setInitialFilterOptions(filterOptions);
        }
    }, [subscriptions, entityRegistry, viewer, ownedAndMemberGroupUrns, initialFilterOptions]);

    // Use the stored initial filter options, or generate them if not yet stored
    const filterOptions =
        initialFilterOptions ||
        extractFilterOptionListFromSubscriptions(subscriptions, entityRegistry, viewer, ownedAndMemberGroupUrns);

    const handleSearchTextChange = (searchText: string) => {
        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, searchText },
        });
    };

    const handleEntityChange = (entities: string[]) => {
        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, entity: entities as EntityType[] },
        });
    };

    const handleOwnerChange = (owners: string[]) => {
        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, owner: owners },
        });
    };

    const handleEventTypeChange = (eventTypes: string[]) => {
        handleFilterChange({
            ...selectedFilters,
            filterCriteria: { ...selectedFilters.filterCriteria, eventType: eventTypes },
        });
    };
    return (
        <SearchFilterContainer>
            {/* ************************* Search Component ************************* */}
            <SearchBar
                value={selectedFilters.filterCriteria?.searchText}
                onChange={handleSearchTextChange}
                placeholder="Search..."
                width="320px"
                height="34px"
                allowClear
                debounceDelay={500}
            />

            {/* ************************* Filter Options ************************* */}
            <FilterOptionsWrapper>
                {/* ************************* Entity Type Filter ************************* */}
                <SimpleSelect
                    width="fit-content"
                    options={
                        filterOptions?.filterGroupOptions?.entity?.map((option) => ({
                            value: option.name,
                            label: option.displayName,
                        })) || []
                    }
                    values={selectedFilters.filterCriteria.entity}
                    onUpdate={(values) => {
                        handleEntityChange(values);
                    }}
                    placeholder="Entity Type"
                    isMultiSelect
                    selectLabelProps={{
                        variant: 'labeled',
                        label: 'Entity Type',
                    }}
                    showClear={false}
                />

                {/* ************************* Owner Filter ************************* */}
                <SimpleSelect
                    width="fit-content"
                    options={
                        filterOptions?.filterGroupOptions?.owner?.map((option) => ({
                            value: option.name,
                            label: option.displayName,
                        })) || []
                    }
                    values={selectedFilters.filterCriteria.owner}
                    onUpdate={(values) => {
                        handleOwnerChange(values);
                    }}
                    placeholder="Owner"
                    isMultiSelect
                    selectLabelProps={{
                        variant: 'labeled',
                        label: 'Owner',
                    }}
                    showClear={false}
                />

                {/* ************************* Event Type Filter ************************* */}
                <SimpleSelect
                    width="fit-content"
                    options={
                        filterOptions?.filterGroupOptions?.eventType?.map((option) => ({
                            value: option.name,
                            label: option.displayName,
                        })) || []
                    }
                    values={selectedFilters.filterCriteria.eventType}
                    onUpdate={(values) => {
                        handleEventTypeChange(values);
                    }}
                    placeholder="Events"
                    isMultiSelect
                    selectLabelProps={{
                        variant: 'labeled',
                        label: 'Events',
                    }}
                    showClear={false}
                />
            </FilterOptionsWrapper>
        </SearchFilterContainer>
    );
};
