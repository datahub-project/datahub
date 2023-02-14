import * as React from 'react';
import { useEffect, useState } from 'react';
import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { SimpleSearchFilter } from './SimpleSearchFilter';

const TOP_FILTERS = ['degree', 'entity', 'platform', 'tags', 'glossaryTerms', 'domains', 'owners'];

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

    const sortedFacets = cachedProps.facets.sort((facetA, facetB) => {
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
