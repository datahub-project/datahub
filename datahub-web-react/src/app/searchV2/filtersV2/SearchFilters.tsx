import React, { useCallback, useMemo, useState } from 'react';

import FiltersRenderingRunner from '@app/searchV2/filtersV2/FiltersRenderingRunner';
import { SearchFiltersProvider } from '@app/searchV2/filtersV2/context';
import DynamicFacetsUpdater from '@app/searchV2/filtersV2/defaults/DefaultFacetsUpdater/DefaultFacetsUpdater';
import DefaultFiltersRenderer from '@app/searchV2/filtersV2/defaults/DefaultFiltersRenderer';
import defaultFiltersRegistry from '@app/searchV2/filtersV2/defaults/defaultFiltersRegistry';
import {
    AppliedFieldFilterUpdater,
    FieldName,
    FieldToAppliedFieldFiltersMap,
    FieldToFacetStateMap,
    FiltersAppliedHandler,
    FiltersRenderer,
} from '@app/searchV2/filtersV2/types';

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

    const onFieldFacetsUpdated = useCallback((facets: FieldToFacetStateMap) => {
        setFieldToFacetStateMap((currentFieldFacets) => new Map([...currentFieldFacets, ...facets]));
    }, []);

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
                onFieldFacetsUpdated={onFieldFacetsUpdated}
            />
            {/* Renders filters */}
            <FiltersRenderingRunner fieldNames={fields} hideEmptyFilters />
        </SearchFiltersProvider>
    );
}
