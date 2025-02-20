import { useState } from 'react';
import { FacetFilterInput } from '../../../types.generated';
import { FilterMode, FilterModes, UnionType } from '../utils/constants';
import { hasAdvancedFilters } from '../utils/hasAdvancedFilters';
import useLatest from '../../shared/useLatest';

const useFilterMode = (filters: Array<FacetFilterInput>, unionType: UnionType) => {
    const onlyShowAdvancedFilters = hasAdvancedFilters(filters, unionType);
    const [filterMode, setFilterMode] = useState<FilterMode>(
        onlyShowAdvancedFilters ? FilterModes.ADVANCED : FilterModes.BASIC,
    );
    const filterModeRef = useLatest(filterMode);
    return { filterMode, filterModeRef, setFilterMode } as const;
};

export default useFilterMode;
