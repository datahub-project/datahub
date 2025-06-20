import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import MoreFilters from '@app/searchV2/filters/MoreFilters';
import SaveViewButton from '@app/searchV2/filters/SaveViewButton';
import SearchFilter from '@app/searchV2/filters/SearchFilter';
import SearchFiltersLoadingSection from '@app/searchV2/filters/SearchFiltersLoadingSection';
import { FILTERS_TO_REMOVE, NON_FACET_FILTER_FIELDS, SORTED_FILTERS } from '@app/searchV2/filters/constants';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { useFilterRendererRegistry } from '@app/searchV2/filters/render/useFilterRenderer';
import { FilterPredicate } from '@app/searchV2/filters/types';
import { convertToAvailableFilterPredictes, sortFacets } from '@app/searchV2/filters/utils';
import { ORIGIN_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import { useAppConfig } from '@src/app/useAppConfig';

import { FacetFilterInput, FacetMetadata } from '@types';

const NUM_VISIBLE_FILTER_DROPDOWNS = 6;

export const FlexWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
`;

export const FlexSpacer = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const FilterButtonsWrapper = styled.div`
    display: flex;
    flex-wrap: nowrap;
`;

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function SearchFilterOptions({
    loading,
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
}: Props) {
    // If we move view select down, then move this down into a sibling component.
    const userContext = useUserContext();
    const filterRendererRegistry = useFilterRendererRegistry();
    const { config } = useAppConfig();
    const fieldsWithCustomRenderers = Array.from(filterRendererRegistry.fieldNameToRenderer.keys());
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;
    const showSaveViewButton = activeFilters?.length > 0 && selectedViewUrn === undefined;

    let filterSet = availableFilters;
    if (filterSet.length) {
        // Include non-facet filters and remove any duplicates in filterSet
        const nonFacetFilters = NON_FACET_FILTER_FIELDS.map(
            ({ field, displayName }): FacetMetadata => ({
                field,
                displayName,
                aggregations: [],
            }),
        );
        const nonFacetFilterKeys = new Set(nonFacetFilters.map((f) => f.field));
        filterSet = [...filterSet.filter((f) => !nonFacetFilterKeys.has(f.field)), ...nonFacetFilters];
    }

    // These are the filter options that originate in the backend
    // They are shown to the user by default.
    const dynamicFilterOptions = filterSet
        ?.filter((f) => (f.field === ORIGIN_FILTER_NAME ? f.aggregations.length >= 2 : true)) // only want Environment filter if there's 2 or more envs
        .filter((f) => !FILTERS_TO_REMOVE.includes(f.field))
        .filter((f) => {
            if (fieldsWithCustomRenderers.includes(f.field)) {
                // If there are no true aggregations, these fields needn't be rendered
                return !!f.aggregations.find((agg) => agg.value === 'true');
            }
            return true;
        })
        .sort((facetA, facetB) => sortFacets(facetA, facetB, SORTED_FILTERS));

    // if there will only be one filter in the "More Filters" dropdown, show that filter instead
    const shouldShowMoreDropdown =
        dynamicFilterOptions && dynamicFilterOptions.length > NUM_VISIBLE_FILTER_DROPDOWNS + 1;
    const visibleFilters = shouldShowMoreDropdown
        ? dynamicFilterOptions?.slice(0, NUM_VISIBLE_FILTER_DROPDOWNS)
        : dynamicFilterOptions;
    const hiddenFilters = shouldShowMoreDropdown ? dynamicFilterOptions?.slice(NUM_VISIBLE_FILTER_DROPDOWNS) : [];

    const filterPredicates: FilterPredicate[] = convertToAvailableFilterPredictes(activeFilters, visibleFilters || []);

    const hiddenFilterPredicates: FilterPredicate[] = convertToAvailableFilterPredictes(
        activeFilters,
        hiddenFilters || [],
    );

    return (
        <FlexSpacer>
            <FlexWrapper>
                {loading && !visibleFilters?.length && <SearchFiltersLoadingSection />}
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
                            filterPredicates={filterPredicates}
                        />
                    );
                })}
                {hiddenFilters && hiddenFilters.length > 0 && (
                    <MoreFilters
                        filters={hiddenFilters}
                        activeFilters={activeFilters}
                        onChangeFilters={onChangeFilters}
                        filterPredicates={hiddenFilterPredicates}
                    />
                )}
            </FlexWrapper>
            <FilterButtonsWrapper>
                {showSaveViewButton && <SaveViewButton activeFilters={activeFilters} unionType={unionType} />}
            </FilterButtonsWrapper>
        </FlexSpacer>
    );
}
