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
    fieldToFacetStateMap: new Map(),
    fieldToAppliedFiltersMap: new Map(),
    filtersRegistry: defaultFiltersRegistry,
    filtersRenderer: DefaultFiltersRenderer,
    updateFieldAppliedFilters: () => null,
});

export default SearchFiltersContext;
