import React, { useCallback, useMemo, useState } from 'react';
<<<<<<< HEAD

import FiltersRenderingRunner from '@app/searchV2/filtersV2/FiltersRenderingRunner';
import { SearchFiltersProvider } from '@app/searchV2/filtersV2/context';
import DynamicFacetsUpdater from '@app/searchV2/filtersV2/defaults/DefaultFacetsUpdater/DefaultFacetsUpdater';
import DefaultFiltersRenderer from '@app/searchV2/filtersV2/defaults/DefaultFiltersRenderer';
import defaultFiltersRegistry from '@app/searchV2/filtersV2/defaults/defaultFiltersRegistry';
=======
import DynamicFacetsUpdater from './defaults/DefaultFacetsUpdater/DefaultFacetsUpdater';
import FiltersRenderingRunner from './FiltersRenderingRunner';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
import {
    AppliedFieldFilterUpdater,
    FieldName,
    FieldToAppliedFieldFiltersMap,
    FieldToFacetStateMap,
    FiltersAppliedHandler,
    FiltersRenderer,
<<<<<<< HEAD
} from '@app/searchV2/filtersV2/types';
=======
} from './types';
import { SearchFiltersProvider } from './context';
import defaultFiltersRegistry from './defaults/defaultFiltersRegistry';
import DefaultFiltersRenderer from './defaults/DefaultFiltersRenderer';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
