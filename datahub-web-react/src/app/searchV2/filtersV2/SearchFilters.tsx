import React, { useEffect, useCallback, useMemo, useState } from 'react';
import DynamicFacetsUpdater from './defaults/DefaultFacetsUpdater/DefaultFacetsUpdater';
import FiltersRenderingRunner from './FiltersRenderingRunner';
import {
    AppliedFieldFilterUpdater,
    FieldName,
    FieldToAppliedFieldFiltersMap,
    FieldToFacetStateMap,
    FiltersAppliedHandler,
    FiltersRenderer,
} from './types';
import { SearchFiltersProvider } from './context';
import defaultFiltersRegistry from './defaults/defaultFiltersRegistry';
import DefaultFiltersRenderer from './defaults/DefaultFiltersRenderer';

interface Props {
    query: string;
    appliedFilters?: FieldToAppliedFieldFiltersMap;
    onFiltersApplied?: FiltersAppliedHandler;
    updateFieldAppliedFilters?: AppliedFieldFilterUpdater;
    fields: FieldName[];
    filtersRenderer?: FiltersRenderer;
    fieldToFacetStateMap?: FieldToFacetStateMap;
}

export default function SearchFilters({
    query,
    onFiltersApplied,
    fields,
    appliedFilters,
    updateFieldAppliedFilters,
    filtersRenderer = DefaultFiltersRenderer,
    fieldToFacetStateMap,
}: Props) {
    const [internalFieldToFacetStateMap, setInternalFieldToFacetStateMap] = useState<FieldToFacetStateMap>(new Map());

    useEffect(() => {
        if (fieldToFacetStateMap !== undefined) setInternalFieldToFacetStateMap(fieldToFacetStateMap);
    }, [fieldToFacetStateMap]);

    const wrappedQuery = useMemo(() => {
        const cleanedQuery = query.trim();
        if (cleanedQuery.length === 0) return cleanedQuery;
        if (cleanedQuery.includes('*')) return cleanedQuery;
        if (cleanedQuery.length < 3 && !cleanedQuery.endsWith('*')) return `${cleanedQuery}*`;
        return query;
    }, [query]);

    const onFieldFacetsUpdated = useCallback((facets: FieldToFacetStateMap) => {
        setInternalFieldToFacetStateMap((currentFieldFacets) => new Map([...currentFieldFacets, ...facets]));
    }, []);

    return (
        <SearchFiltersProvider
            fields={fields}
            fieldToAppliedFiltersMap={appliedFilters}
            fieldToFacetStateMap={internalFieldToFacetStateMap}
            filtersRegistry={defaultFiltersRegistry}
            onFiltersApplied={onFiltersApplied}
            updateFieldAppliedFilters={updateFieldAppliedFilters}
            filtersRenderer={filtersRenderer}
        >
            {/* Updates facets depending on query and applied filters if they are not controlled by `fieldToFacetStateMap` */}
            {fieldToFacetStateMap === undefined && (
                <DynamicFacetsUpdater
                    fieldNames={fields}
                    query={wrappedQuery}
                    onFieldFacetsUpdated={onFieldFacetsUpdated}
                />
            )}
            {/* Renders filters */}
            <FiltersRenderingRunner fieldNames={fields} hideEmptyFilters />
        </SearchFiltersProvider>
    );
}
