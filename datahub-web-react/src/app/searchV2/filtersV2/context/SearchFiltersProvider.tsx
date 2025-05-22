import React, { useEffect } from 'react';

import SearchFiltersContext from '@app/searchV2/filtersV2/context/SearchFiltersContext';
import DefaultFiltersRenderer from '@app/searchV2/filtersV2/defaults/DefaultFiltersRenderer';
import defaultFiltersRegistry from '@app/searchV2/filtersV2/defaults/defaultFiltersRegistry';
import FiltersRegistry from '@app/searchV2/filtersV2/filtersRegistry/filtersRegistry';
import {
    AppliedFieldFilterUpdater,
    FieldName,
    FieldToAppliedFieldFiltersMap,
    FieldToFacetStateMap,
    FiltersAppliedHandler,
    FiltersRenderer,
} from '@app/searchV2/filtersV2/types';

export interface Props {
    fields: FieldName[];
    viewUrn?: string | null;
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
    viewUrn,
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
                viewUrn,
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
