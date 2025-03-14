import React, { useMemo, useState } from 'react';
import DynamicFacetsUpdater from './defaults/DefaultFacetsUpdater/DefaultFacetsUpdater';
import FiltersRenderingRunner from './FiltersRenderingRunner';
import { FieldName, FieldToFacetStateMap, FiltersAppliedHandler } from './types';
import { SearchFiltersProvider } from './context';
import defaultFiltersRegistry from './defaults/defaultFiltersRegistry';

interface Props {
    query: string;
    onFiltersApplied?: FiltersAppliedHandler;
    fields: FieldName[];
}

export default function SearchFilters({ query, onFiltersApplied, fields }: Props) {
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
            fieldToFacetStateMap={fieldToFacetStateMap}
            filtersRegistry={defaultFiltersRegistry}
            onFiltersApplied={onFiltersApplied}
        >
            <DynamicFacetsUpdater
                fieldNames={fields}
                query={wrappedQuery}
                onFieldFacetsUpdated={(map) => setFieldToFacetStateMap(map)}
            />
            <FiltersRenderingRunner fieldNames={fields} hideEmptyFilters />
        </SearchFiltersProvider>
    );
}
