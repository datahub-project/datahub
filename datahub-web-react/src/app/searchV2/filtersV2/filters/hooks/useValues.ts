/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { AppliedFieldFilterValue } from '@src/app/searchV2/filtersV2/types';

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
