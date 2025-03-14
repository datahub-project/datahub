import React, { useCallback, useEffect, useState } from 'react';
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
    defaultAppliedFilters?: FieldToAppliedFieldFiltersMap;
    fieldToFacetStateMap: FieldToFacetStateMap;
    filtersRenderer?: FiltersRenderer;
    filtersRegistry?: FiltersRegistry;
    onFiltersApplied?: FiltersAppliedHandler;
}

export default function SearchFiltersProvider({
    children,
    fields,
    defaultAppliedFilters,
    fieldToFacetStateMap,
    filtersRegistry = defaultFiltersRegistry,
    filtersRenderer = DefaultFiltersRenderer,
    onFiltersApplied,
}: React.PropsWithChildren<Props>) {
    const [fieldToAppliedFiltersMap, setFieldToAppliedFiltersMap] = useState<FieldToAppliedFieldFiltersMap>(
        new Map(defaultAppliedFilters),
    );

    const applyFilter: AppliedFieldFilterUpdater = useCallback((fieldName, value) => {
        setFieldToAppliedFiltersMap((prevAppliedFilters) => {
            const filters = value.filters
                .filter((input) => input.field === fieldName)
                .filter((input) => input.values && input.values.length > 0);

            return new Map([...prevAppliedFilters, [fieldName, { filters }]]);
        });
    }, []);

    useEffect(() => onFiltersApplied?.(fieldToAppliedFiltersMap), [onFiltersApplied, fieldToAppliedFiltersMap]);

    return (
        <SearchFiltersContext.Provider
            value={{
                fieldToFacetStateMap,
                fields,
                fieldToAppliedFiltersMap,
                filtersRegistry,
                filtersRenderer,
                updateFieldAppliedFilters: applyFilter,
            }}
        >
            {children}
        </SearchFiltersContext.Provider>
    );
}
