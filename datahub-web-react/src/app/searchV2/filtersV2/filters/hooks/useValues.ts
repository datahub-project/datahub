<<<<<<< HEAD
import { useMemo } from 'react';

import { AppliedFieldFilterValue } from '@src/app/searchV2/filtersV2/types';
=======
import { AppliedFieldFilterValue } from '@src/app/searchV2/filtersV2/types';
import { useMemo } from 'react';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function useValues(appliedFilters: AppliedFieldFilterValue | undefined): string[] {
    const values = useMemo(
        () =>
            appliedFilters?.filters
                ?.map((filter) => filter.values)
                .flat()
                .filter((value): value is string => !!value) ?? [],
        [appliedFilters],
    );

    return values;
}
