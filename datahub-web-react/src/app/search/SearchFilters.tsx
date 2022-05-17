import * as React from 'react';
import styled from 'styled-components';
import { useEffect, useState } from 'react';
import { FacetMetadata } from '../../types.generated';
import { SearchFilter } from './SearchFilter';

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
    selectedFilters: Array<{
        field: string;
        value: string;
    }>;
    onFilterSelect: (
        newFilters: Array<{
            field: string;
            value: string;
        }>,
    ) => void;
    loading: boolean;
}

export const SearchFilters = ({ facets, selectedFilters, onFilterSelect, loading }: Props) => {
    const [cachedProps, setCachedProps] = useState<{
        facets: Array<FacetMetadata>;
        selectedFilters: Array<{
            field: string;
            value: string;
        }>;
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
            ? [...selectedFilters, { field, value }]
            : selectedFilters.filter((filter) => filter.field !== field || filter.value !== value);
        setCachedProps({ ...cachedProps, selectedFilters: newFilters });
        onFilterSelect(newFilters);
    };

    return (
        <SearchFilterWrapper>
            {cachedProps.facets.map((facet) => (
                <SearchFilter
                    key={`${facet.displayName}-${facet.field}`}
                    facet={facet}
                    selectedFilters={cachedProps.selectedFilters}
                    onFilterSelect={onFilterSelectAndSetCache}
                />
            ))}
        </SearchFilterWrapper>
    );
};
