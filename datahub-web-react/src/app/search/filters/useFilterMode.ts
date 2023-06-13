import { useState } from 'react';
import { FacetFilterInput } from '../../../types.generated';
import { UnionType } from '../utils/constants';
import { hasAdvancedFilters } from '../utils/hasAdvancedFilters';
import { FilterMode } from '../utils/types';

const useFilterMode = (filters: Array<FacetFilterInput>, unionType: UnionType) => {
    const onlyShowAdvancedFilters = hasAdvancedFilters(filters, unionType);
    const [filterMode, setFilterMode] = useState<FilterMode>(onlyShowAdvancedFilters ? 'advanced' : 'basic');
    return { filterMode, setFilterMode } as const;
};

export default useFilterMode;
