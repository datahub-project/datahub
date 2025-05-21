import { useState } from 'react';

import { FilterMode, FilterModes, UnionType } from '@app/search/utils/constants';
import { hasAdvancedFilters } from '@app/search/utils/hasAdvancedFilters';
import useLatest from '@app/shared/useLatest';

import { FacetFilterInput } from '@types';

const useFilterMode = (filters: Array<FacetFilterInput>, unionType: UnionType) => {
    const onlyShowAdvancedFilters = hasAdvancedFilters(filters, unionType);
    const [filterMode, setFilterMode] = useState<FilterMode>(
        onlyShowAdvancedFilters ? FilterModes.ADVANCED : FilterModes.BASIC,
    );
    const filterModeRef = useLatest(filterMode);
    return { filterMode, filterModeRef, setFilterMode } as const;
};

export default useFilterMode;
