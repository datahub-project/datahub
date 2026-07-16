import { useMemo } from 'react';

import { convertFiltersToFiltersMap } from '@app/searchV2/filtersV2/utils';
import useQueryAndFiltersFromLocation from '@app/searchV2/useQueryAndFiltersFromLocation';

export default function useFiltersMapFromQueryParams() {
    const { filters } = useQueryAndFiltersFromLocation();

    const filtersMap = useMemo(() => convertFiltersToFiltersMap(filters), [filters]);

    return filtersMap;
}
