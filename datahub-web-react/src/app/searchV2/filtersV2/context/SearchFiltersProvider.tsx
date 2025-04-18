import React, { useEffect } from 'react';
<<<<<<< HEAD

import SearchFiltersContext from '@app/searchV2/filtersV2/context/SearchFiltersContext';
import DefaultFiltersRenderer from '@app/searchV2/filtersV2/defaults/DefaultFiltersRenderer';
import defaultFiltersRegistry from '@app/searchV2/filtersV2/defaults/defaultFiltersRegistry';
import FiltersRegistry from '@app/searchV2/filtersV2/filtersRegistry/filtersRegistry';
=======
import DefaultFiltersRenderer from '../defaults/DefaultFiltersRenderer';
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
} from '../types';
import SearchFiltersContext from './SearchFiltersContext';
import FiltersRegistry from '../filtersRegistry/filtersRegistry';
import defaultFiltersRegistry from '../defaults/defaultFiltersRegistry';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export interface Props {
    fields: FieldName[];
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
