import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import {
    SEARCH_RESULTS_ADVANCED_SEARCH_ID,
    SEARCH_RESULTS_FILTERS_ID,
} from '@app/onboarding/config/SearchOnboardingConfig';
import ActiveFilter from '@app/search/filters/ActiveFilter';
import BasicFiltersLoadingSection from '@app/search/filters/BasicFiltersLoadingSection';
import MoreFilters from '@app/search/filters/MoreFilters';
import SaveViewButton from '@app/search/filters/SaveViewButton';
import SearchFilter from '@app/search/filters/SearchFilter';
import { SORTED_FILTERS } from '@app/search/filters/constants';
import { FilterScenarioType } from '@app/search/filters/render/types';
import { useFilterRendererRegistry } from '@app/search/filters/render/useFilterRenderer';
import { TextButton } from '@app/search/filters/styledComponents';
import { sortFacets } from '@app/search/filters/utils';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    UnionType,
} from '@app/search/utils/constants';
import { useAppConfig } from '@src/app/useAppConfig';

import { FacetFilterInput, FacetMetadata } from '@types';

const NUM_VISIBLE_FILTER_DROPDOWNS = 5;

export const FlexWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
`;

export const FlexSpacer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const StyledDivider = styled(Divider)`
    margin: 8px 0 0 0;
`;

export const FilterButtonsWrapper = styled.div`
    display: flex;
    flex-wrap: nowrap;
`;

// remove legacy filter options as well as new _index and browsePathV2 filter from dropdowns
const FILTERS_TO_REMOVE = [
    TYPE_NAMES_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    BROWSE_PATH_V2_FILTER_NAME,
];

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onClearFilters: () => void;
    showAdvancedFilters: () => void;
}

export default function BasicFilters({
    loading,
    availableFilters,
    activeFilters,
    onChangeFilters,
    onClearFilters,
    showAdvancedFilters,
}: Props) {
    const userContext = useUserContext();
    const { config } = useAppConfig();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;
    const showSaveViewButton = activeFilters?.length > 0 && selectedViewUrn === undefined;
    // only want Environment filter if there's 2 or more envs
    const filters = availableFilters
        ?.filter((f) => (f.field === ORIGIN_FILTER_NAME ? f.aggregations.length >= 2 : true)) // only want Environment filter if there's 2 or more envs
        .filter((f) => !FILTERS_TO_REMOVE.includes(f.field))
        .sort((facetA, facetB) => sortFacets(facetA, facetB, SORTED_FILTERS));
    // if there will only be one filter in the "More Filters" dropdown, show that filter instead
    const shouldShowMoreDropdown = filters && filters.length > NUM_VISIBLE_FILTER_DROPDOWNS + 1;
    const visibleFilters = shouldShowMoreDropdown ? filters?.slice(0, NUM_VISIBLE_FILTER_DROPDOWNS) : filters;
    const hiddenFilters = shouldShowMoreDropdown ? filters?.slice(NUM_VISIBLE_FILTER_DROPDOWNS) : [];
    const filterRendererRegistry = useFilterRendererRegistry();

    return (
        <span id={SEARCH_RESULTS_FILTERS_ID}>
            <FlexSpacer>
                <FlexWrapper>
                    {loading && !visibleFilters?.length && <BasicFiltersLoadingSection />}
                    {visibleFilters?.map((filter) => {
                        return filterRendererRegistry.hasRenderer(filter.field) ? (
                            filterRendererRegistry.render(filter.field, {
                                scenario: FilterScenarioType.SEARCH_V2_PRIMARY,
                                filter,
                                activeFilters,
                                onChangeFilters,
                                config,
                            })
                        ) : (
                            <SearchFilter
                                key={filter.field}
                                filter={filter}
                                activeFilters={activeFilters}
                                onChangeFilters={onChangeFilters}
                            />
                        );
                    })}
                    {hiddenFilters && hiddenFilters.length > 0 && (
                        <MoreFilters
                            filters={hiddenFilters}
                            activeFilters={activeFilters}
                            onChangeFilters={onChangeFilters}
                        />
                    )}
                </FlexWrapper>
                <FilterButtonsWrapper>
                    {showSaveViewButton && <SaveViewButton activeFilters={activeFilters} unionType={UnionType.AND} />}
                    <TextButton
                        id={SEARCH_RESULTS_ADVANCED_SEARCH_ID}
                        type="text"
                        onClick={showAdvancedFilters}
                        marginTop={0}
                    >
                        Advanced Filters
                    </TextButton>
                </FilterButtonsWrapper>
            </FlexSpacer>
            {activeFilters.length > 0 && (
                <>
                    <StyledDivider />
                    <FlexSpacer>
                        <FlexWrapper>
                            {activeFilters.map((activeFilter) => (
                                <ActiveFilter
                                    key={activeFilter.field}
                                    filter={activeFilter}
                                    availableFilters={availableFilters}
                                    activeFilters={activeFilters}
                                    onChangeFilters={onChangeFilters}
                                />
                            ))}
                        </FlexWrapper>
                        <TextButton type="text" onClick={onClearFilters} height={14} data-testid="clear-all-filters">
                            clear all
                        </TextButton>
                    </FlexSpacer>
                </>
            )}
        </span>
    );
}
