import * as React from 'react';
import { FacetMetadata } from '../../types.generated';
import { SearchFilter } from './SearchFilter';

interface Props {
    facets: Array<FacetMetadata>;
    selectedFilters: Array<{
        field: string;
        value: string;
    }>;
    onFilterSelect: (selected: boolean, field: string, value: string) => void;
}

export const SearchFilters = ({ facets, selectedFilters, onFilterSelect }: Props) => {
    return (
        <>
            {facets.map((facet) => (
                <SearchFilter facet={facet} selectedFilters={selectedFilters} onFilterSelect={onFilterSelect} />
            ))}
        </>
    );
};
