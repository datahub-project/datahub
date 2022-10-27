import * as React from 'react';
import styled from 'styled-components';
import { useEffect, useState } from 'react';
import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { SimpleSearchFilter } from './SimpleSearchFilter';

const TOP_FILTERS = ['degree', 'entity', 'tags', 'glossaryTerms', 'domains', 'owners'];

export const SearchFilterWrapper = styled.div`
    max-height: 100%;
    overflow: auto;

    &::-webkit-scrollbar {
        height: 12px;
        width: 1px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;

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
        <SearchFilterWrapper>
            {sortedFacets.map((facet) => (
                <SimpleSearchFilter
                    key={`${facet.displayName}-${facet.field}`}
                    facet={facet}
                    selectedFilters={cachedProps.selectedFilters}
                    onFilterSelect={onFilterSelectAndSetCache}
                    defaultDisplayFilters={TOP_FILTERS.includes(facet.field)}
                />
            ))}
        </SearchFilterWrapper>
    );
};
