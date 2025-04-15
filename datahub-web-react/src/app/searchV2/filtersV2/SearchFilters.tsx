import React, { useCallback, useEffect, useMemo, useState } from 'react';

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
import { MIN_CHARACTER_COUNT_FOR_SEARCH } from '@app/searchV2/utils/constants';

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
        if (cleanedQuery.length < MIN_CHARACTER_COUNT_FOR_SEARCH && !cleanedQuery.endsWith('*'))
            return `${cleanedQuery}*`;
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
