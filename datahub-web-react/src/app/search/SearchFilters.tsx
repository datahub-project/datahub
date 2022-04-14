import * as React from 'react';
import { useEffect, useState } from 'react';
import { FacetMetadata } from '../../types.generated';
import { SearchFilter } from './SearchFilter';

type ListStyle = {
    height: string;
};
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
    style?: ListStyle | null;
}

export const SearchFilters = ({ facets, selectedFilters, onFilterSelect, loading, style }: Props) => {
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
        <div style={style ? { height: style.height, overflowY: 'auto' } : {}}>
            {cachedProps.facets.map((facet) => (
                <SearchFilter
                    key={`${facet.displayName}-${facet.field}`}
                    facet={facet}
                    selectedFilters={cachedProps.selectedFilters}
                    onFilterSelect={onFilterSelectAndSetCache}
                />
            ))}
        </div>
    );
};
