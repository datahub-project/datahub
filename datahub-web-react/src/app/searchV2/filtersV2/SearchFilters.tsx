/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    viewUrn?: string | null;
    appliedFilters?: FieldToAppliedFieldFiltersMap;
    onFiltersApplied?: FiltersAppliedHandler;
    updateFieldAppliedFilters?: AppliedFieldFilterUpdater;
    fields: FieldName[];
    filtersRenderer?: FiltersRenderer;
    fieldToFacetStateMap?: FieldToFacetStateMap;
    shouldUpdateFacetsForFieldsWithAppliedFilters?: boolean;
    shouldUpdateFacetsForFieldsWithoutAppliedFilters?: boolean;
}

export default function SearchFilters({
    query,
    viewUrn,
    onFiltersApplied,
    fields,
    appliedFilters,
    updateFieldAppliedFilters,
    filtersRenderer = DefaultFiltersRenderer,
    fieldToFacetStateMap,
    shouldUpdateFacetsForFieldsWithAppliedFilters,
    shouldUpdateFacetsForFieldsWithoutAppliedFilters,
}: Props) {
    const [internalFieldToFacetStateMap, setInternalFieldToFacetStateMap] = useState<FieldToFacetStateMap>(new Map());
    const [dynamicFieldToFacetStateMap, setDynamicFieldToFacetStateMap] = useState<FieldToFacetStateMap>(new Map());

    useEffect(() => {
        setInternalFieldToFacetStateMap(new Map([...(fieldToFacetStateMap ?? []), ...dynamicFieldToFacetStateMap]));
    }, [fieldToFacetStateMap, dynamicFieldToFacetStateMap]);

    const wrappedQuery = useMemo(() => {
        const cleanedQuery = query.trim();
        if (cleanedQuery.length === 0) return cleanedQuery;
        if (cleanedQuery.includes('*')) return cleanedQuery;
        if (cleanedQuery.length < MIN_CHARACTER_COUNT_FOR_SEARCH && !cleanedQuery.endsWith('*'))
            return `${cleanedQuery}*`;
        return query;
    }, [query]);

    const onFieldFacetsUpdated = useCallback((facets: FieldToFacetStateMap) => {
        setDynamicFieldToFacetStateMap((currentFieldFacets) => new Map([...currentFieldFacets, ...facets]));
    }, []);

    return (
        <SearchFiltersProvider
            fields={fields}
            viewUrn={viewUrn}
            fieldToAppliedFiltersMap={appliedFilters}
            fieldToFacetStateMap={internalFieldToFacetStateMap}
            filtersRegistry={defaultFiltersRegistry}
            onFiltersApplied={onFiltersApplied}
            updateFieldAppliedFilters={updateFieldAppliedFilters}
            filtersRenderer={filtersRenderer}
        >
            {/* Updates facets depending on query and applied filters */}
            <DynamicFacetsUpdater
                fieldNames={fields}
                query={wrappedQuery}
                viewUrn={viewUrn}
                onFieldFacetsUpdated={onFieldFacetsUpdated}
                shouldUpdateFacetsForFieldsWithAppliedFilters={shouldUpdateFacetsForFieldsWithAppliedFilters}
                shouldUpdateFacetsForFieldsWithoutAppliedFilters={shouldUpdateFacetsForFieldsWithoutAppliedFilters}
            />
            {/* Renders filters */}
            <FiltersRenderingRunner fieldNames={fields} hideEmptyFilters />
        </SearchFiltersProvider>
    );
}
