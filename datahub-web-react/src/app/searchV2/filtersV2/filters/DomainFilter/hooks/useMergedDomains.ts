/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { domainKeyAccessor } from '@app/searchV2/filtersV2/filters/DomainFilter/utils';
import { getUniqueItemsByKeyFromArrrays } from '@app/searchV2/filtersV2/utils';
import { Domain } from '@src/types.generated';

export default function useMergedDomains(
    domainsFromAppliedFilters: Domain[],
    domainsFromSuggestions: Domain[],
    domainsFromAggregations: Domain[],
): Domain[] {
    return useMemo(
        () =>
            getUniqueItemsByKeyFromArrrays(
                [domainsFromAppliedFilters, domainsFromSuggestions, domainsFromAggregations],
                domainKeyAccessor,
            ),
        [domainsFromAppliedFilters, domainsFromSuggestions, domainsFromAggregations],
    );
}
