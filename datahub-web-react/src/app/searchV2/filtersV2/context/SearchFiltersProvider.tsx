/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
