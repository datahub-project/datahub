/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { convertFiltersToFiltersMap } from '@app/searchV2/filtersV2/utils';
import useQueryAndFiltersFromLocation from '@app/searchV2/useQueryAndFiltersFromLocation';

export default function useFiltersMapFromQueryParams() {
    const { filters } = useQueryAndFiltersFromLocation();

    const filtersMap = useMemo(() => convertFiltersToFiltersMap(filters), [filters]);

    return filtersMap;
}
