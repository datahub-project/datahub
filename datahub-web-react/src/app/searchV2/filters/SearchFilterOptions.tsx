import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { SlidersOutlined } from '@ant-design/icons';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { useUserContext } from '../../context/useUserContext';
import { ORIGIN_FILTER_NAME, UnionType } from '../utils/constants';
import { FILTERS_TO_REMOVE, NON_FACET_FILTER_FIELDS, SORTED_FILTERS } from './constants';
import MoreFilters from './MoreFilters';
import SaveViewButton from './SaveViewButton';
import SearchFilter from './SearchFilter';
import { convertToAvailableFilterPredictes, sortFacets } from './utils';
import { useFilterRendererRegistry } from './render/useFilterRenderer';
import { FilterScenarioType } from './render/types';
import SearchFiltersLoadingSection from './SearchFiltersLoadingSection';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { FilterPredicate } from './types';

const NUM_VISIBLE_FILTER_DROPDOWNS = 6;

const FiltersText = styled.div`
    font-size: 16px;
    color: ${ANTD_GRAY[8]};
`;

const StyledSlidersOutlined = styled(SlidersOutlined)`
    margin-right: 8px;
`;

const VerticalDivider = styled(Divider)`
    && {
        padding: 12px 0px;
        margin: 0px 32px;
    }
`;

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
                <FiltersText>
                    <StyledSlidersOutlined />
                    Filters
                </FiltersText>
                <VerticalDivider type="vertical" />
                {loading && !visibleFilters?.length && <SearchFiltersLoadingSection />}
                {visibleFilters?.map((filter) => {
                    return filterRendererRegistry.hasRenderer(filter.field) ? (
                        filterRendererRegistry.render(filter.field, {
                            scenario: FilterScenarioType.SEARCH_V2_PRIMARY,
                            filter,
                            activeFilters,
                            onChangeFilters,
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
