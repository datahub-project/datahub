/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
