import { useCallback, useMemo, useState } from 'react';
<<<<<<< HEAD

import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
=======
import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '../types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function useAppliedFilters(defaultAppliedFilters?: FieldToAppliedFieldFiltersMap) {
    const [appliedFilters, setAppliedFilters] = useState<FieldToAppliedFieldFiltersMap | undefined>(
        defaultAppliedFilters,
    );

    const clear = useCallback(() => {
        setAppliedFilters(new Map());
    }, []);

    const updateFieldFilters: AppliedFieldFilterUpdater = useCallback((fieldName, value) => {
        setAppliedFilters((prevAppliedFilters) => {
            const filters = value.filters
                .filter((input) => input.field === fieldName)
                .filter((input) => input.values && input.values.length > 0);

            return new Map([...(prevAppliedFilters ?? []), [fieldName, { filters }]]);
        });
    }, []);

    const flatAppliedFilters = useMemo(
        () =>
            Array.from(appliedFilters?.entries?.() || [])
                .map(([_, value]) => value.filters)
                .filter((filters) => filters.length > 0)
                .flat(),
        [appliedFilters],
    );

    const hasAppliedFilters = useMemo(() => flatAppliedFilters.length > 0, [flatAppliedFilters]);

    return { appliedFilters, hasAppliedFilters, flatAppliedFilters, clear, updateFieldFilters };
}
