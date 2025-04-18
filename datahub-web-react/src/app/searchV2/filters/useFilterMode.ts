import { useState } from 'react';

import { FilterMode, FilterModes, UnionType } from '@app/searchV2/utils/constants';
import { hasAdvancedFilters } from '@app/searchV2/utils/hasAdvancedFilters';
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
