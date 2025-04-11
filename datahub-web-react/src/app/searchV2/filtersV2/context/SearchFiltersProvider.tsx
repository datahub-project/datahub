import React, { useEffect } from 'react';
import DefaultFiltersRenderer from '../defaults/DefaultFiltersRenderer';
import {
    AppliedFieldFilterUpdater,
    FieldName,
    FieldToAppliedFieldFiltersMap,
    FieldToFacetStateMap,
    FiltersAppliedHandler,
    FiltersRenderer,
} from '../types';
import SearchFiltersContext from './SearchFiltersContext';
import FiltersRegistry from '../filtersRegistry/filtersRegistry';
import defaultFiltersRegistry from '../defaults/defaultFiltersRegistry';

export interface Props {
    fields: FieldName[];
    fieldToAppliedFiltersMap?: FieldToAppliedFieldFiltersMap;
    filtersRenderer?: FiltersRenderer;
    onFiltersApplied?: FiltersAppliedHandler;
    updateFieldAppliedFilters?: AppliedFieldFilterUpdater;
    fieldToFacetStateMap: FieldToFacetStateMap;
    filtersRegistry?: FiltersRegistry;
}

export default function SearchFiltersProvider({
    children,
    fields,
    fieldToAppliedFiltersMap,
    fieldToFacetStateMap,
    filtersRegistry = defaultFiltersRegistry,
    filtersRenderer = DefaultFiltersRenderer,
    onFiltersApplied,
    updateFieldAppliedFilters,
}: React.PropsWithChildren<Props>) {
    useEffect(() => onFiltersApplied?.(fieldToAppliedFiltersMap), [onFiltersApplied, fieldToAppliedFiltersMap]);

    return (
        <SearchFiltersContext.Provider
            value={{
                fieldToFacetStateMap,
                fields,
                fieldToAppliedFiltersMap,
                filtersRegistry,
                filtersRenderer,
                updateFieldAppliedFilters,
            }}
        >
            {children}
        </SearchFiltersContext.Provider>
    );
}
