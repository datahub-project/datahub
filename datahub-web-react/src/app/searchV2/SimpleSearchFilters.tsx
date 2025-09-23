import * as React from 'react';
import { useEffect, useState } from 'react';

import { SCHEMA_FIELD_ALIASES_FILTER_NAME } from '@app/search/utils/constants';
import { SimpleSearchFilter } from '@app/searchV2/SimpleSearchFilter';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { useFilterRendererRegistry } from '@app/searchV2/filters/render/useFilterRenderer';
import {
    DEGREE_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { useAppConfig } from '@app/useAppConfig';

import { FacetFilterInput, FacetMetadata } from '@types';

const TOP_FILTERS = ['degree', ENTITY_FILTER_NAME, 'platform', 'tags', 'glossaryTerms', 'domains', 'owners'];

const FILTERS_TO_EXCLUDE = [
    LEGACY_ENTITY_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    SCHEMA_FIELD_ALIASES_FILTER_NAME,
];

interface Props {
    facets: Array<FacetMetadata>;
    selectedFilters: Array<FacetFilterInput>;
    onFilterSelect: (newFilters: Array<FacetFilterInput>) => void;
    loading: boolean;
}

export const SimpleSearchFilters = ({ facets, selectedFilters, onFilterSelect, loading }: Props) => {
    const { config } = useAppConfig();
    const [cachedProps, setCachedProps] = useState<{
        facets: Array<FacetMetadata>;
        selectedFilters: Array<FacetFilterInput>;
    }>({
        facets,
        selectedFilters,
    });

    // we want to persist the selected filters through the loading jitter
    useEffect(() => {
        if (!loading) {
            setCachedProps({ facets, selectedFilters });
        }
    }, [facets, selectedFilters, loading]);

    const onFilterSelectAndSetCache = (selected: boolean, field: string, value: string) => {
        const newFilters = selected
            ? [...selectedFilters, { field, values: [value] }]
            : selectedFilters
                  .map((filter) =>
                      filter.field === field
                          ? { ...filter, values: filter.values?.filter((val) => val !== value) }
                          : filter,
                  )
                  .filter((filter) => filter.field !== field || !(filter.values?.length === 0));

        // Do not let user unselect all degree filters
        if (field === DEGREE_FILTER_NAME && !selected) {
            const hasDegreeFilter = newFilters.find((filter) => filter.field === DEGREE_FILTER_NAME);
            if (!hasDegreeFilter) {
                return;
            }
        }

        setCachedProps({ ...cachedProps, selectedFilters: newFilters });
        onFilterSelect(newFilters);
    };

    const filteredFacets = cachedProps.facets.filter((facet) => !FILTERS_TO_EXCLUDE.includes(facet.field));

    const sortedFacets = filteredFacets.sort((facetA, facetB) => {
        if (TOP_FILTERS.indexOf(facetA.field) === -1) return 1;
        if (TOP_FILTERS.indexOf(facetB.field) === -1) return -1;
        return TOP_FILTERS.indexOf(facetA.field) - TOP_FILTERS.indexOf(facetB.field);
    });

    const filterRendererRegistry = useFilterRendererRegistry();

    return (
        <>
            {sortedFacets.map((facet) => {
                return filterRendererRegistry.hasRenderer(facet.field) ? (
                    filterRendererRegistry.render(facet.field, {
                        scenario: FilterScenarioType.SEARCH_V1,
                        filter: facet,
                        activeFilters: selectedFilters,
                        onChangeFilters: onFilterSelect,
                        config,
                    })
                ) : (
                    <SimpleSearchFilter
                        key={`${facet.displayName}-${facet.field}`}
                        facet={facet}
                        selectedFilters={cachedProps.selectedFilters}
                        onFilterSelect={onFilterSelectAndSetCache}
                        defaultDisplayFilters={TOP_FILTERS.includes(facet.field)}
                    />
                );
            })}
        </>
    );
};
