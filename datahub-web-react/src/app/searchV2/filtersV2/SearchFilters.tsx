import React, { useMemo, useState } from 'react';
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
}

export default function SearchFilters({
    query,
    onFiltersApplied,
    fields,
    appliedFilters,
    updateFieldAppliedFilters,
    filtersRenderer = DefaultFiltersRenderer,
}: Props) {
    const [fieldToFacetStateMap, setFieldToFacetStateMap] = useState<FieldToFacetStateMap>(new Map());

    const wrappedQuery = useMemo(() => {
        const cleanedQuery = query.trim();
        if (cleanedQuery.length === 0) return cleanedQuery;
        if (cleanedQuery.includes('*')) return cleanedQuery;
        if (cleanedQuery.length < 3 && !cleanedQuery.endsWith('*')) return `${cleanedQuery}*`;
        return query;
    }, [query]);

    return (
        <SearchFiltersProvider
            fields={fields}
            fieldToAppliedFiltersMap={appliedFilters}
            fieldToFacetStateMap={fieldToFacetStateMap}
            filtersRegistry={defaultFiltersRegistry}
            onFiltersApplied={onFiltersApplied}
            updateFieldAppliedFilters={updateFieldAppliedFilters}
            filtersRenderer={filtersRenderer}
        >
            {/* Updates facets depending on query and applied filters */}
            <DynamicFacetsUpdater
                fieldNames={fields}
                query={wrappedQuery}
                onFieldFacetsUpdated={(map) => setFieldToFacetStateMap(map)}
            />
            {/* Renders filters */}
            <FiltersRenderingRunner fieldNames={fields} hideEmptyFilters />
        </SearchFiltersProvider>
    );
}
