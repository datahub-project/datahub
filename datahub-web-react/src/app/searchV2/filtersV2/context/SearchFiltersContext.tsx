/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import DefaultFiltersRenderer from '@app/searchV2/filtersV2/defaults/DefaultFiltersRenderer';
import defaultFiltersRegistry from '@app/searchV2/filtersV2/defaults/defaultFiltersRegistry';
import FiltersRegistry from '@app/searchV2/filtersV2/filtersRegistry/filtersRegistry';
import {
    AppliedFieldFilterUpdater,
    FieldName,
    FieldToAppliedFieldFiltersMap,
    FieldToFacetStateMap,
    FiltersRenderer,
} from '@app/searchV2/filtersV2/types';

export type SearchFiltersContextType = {
    // Fields to show in filters
    fields: FieldName[];
    // Applied view
    viewUrn?: string | null;
    // State of facets for each field
    fieldToFacetStateMap?: FieldToFacetStateMap;
    // State of applied filters
    fieldToAppliedFiltersMap?: FieldToAppliedFieldFiltersMap;
    // Registry with mapping of field name to filter component
    filtersRegistry: FiltersRegistry;
    // Renderer takes all filters and render them together
    filtersRenderer: FiltersRenderer;
    // Callback called when applied filters updated
    updateFieldAppliedFilters?: AppliedFieldFilterUpdater;
};

const SearchFiltersContext = React.createContext<SearchFiltersContextType>({
    fields: [],
    viewUrn: undefined,
    fieldToFacetStateMap: new Map(),
    fieldToAppliedFiltersMap: new Map(),
    filtersRegistry: defaultFiltersRegistry,
    filtersRenderer: DefaultFiltersRenderer,
    updateFieldAppliedFilters: () => null,
});

export default SearchFiltersContext;
