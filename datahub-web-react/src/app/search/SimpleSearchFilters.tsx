import * as React from 'react';
import { useEffect, useState } from 'react';
import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { SimpleSearchFilter } from './SimpleSearchFilter';
import { ENTITY_FILTER_NAME, ENTITY_INDEX_FILTER_NAME, LEGACY_ENTITY_FILTER_NAME } from './utils/constants';

const TOP_FILTERS = ['degree', ENTITY_FILTER_NAME, 'platform', 'tags', 'glossaryTerms', 'domains', 'owners'];

const FILTERS_TO_EXCLUDE = [LEGACY_ENTITY_FILTER_NAME, ENTITY_INDEX_FILTER_NAME];

interface Props {
    facets: Array<FacetMetadata>;
    selectedFilters: Array<FacetFilterInput>;
    onFilterSelect: (newFilters: Array<FacetFilterInput>) => void;
    loading: boolean;
}

export const SimpleSearchFilters = ({ facets, selectedFilters, onFilterSelect, loading }: Props) => {
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
        setCachedProps({ ...cachedProps, selectedFilters: newFilters });
        onFilterSelect(newFilters);
    };

    const filteredFacets = cachedProps.facets.filter((facet) => FILTERS_TO_EXCLUDE.indexOf(facet.field) === -1);

    const sortedFacets = filteredFacets.sort((facetA, facetB) => {
        if (TOP_FILTERS.indexOf(facetA.field) === -1) return 1;
        if (TOP_FILTERS.indexOf(facetB.field) === -1) return -1;
        return TOP_FILTERS.indexOf(facetA.field) - TOP_FILTERS.indexOf(facetB.field);
    });

    return (
        <>
            {sortedFacets.map((facet) => (
                <SimpleSearchFilter
                    key={`${facet.displayName}-${facet.field}`}
                    facet={facet}
                    selectedFilters={cachedProps.selectedFilters}
                    onFilterSelect={onFilterSelectAndSetCache}
                    defaultDisplayFilters={TOP_FILTERS.includes(facet.field)}
                />
            ))}
        </>
    );
};
