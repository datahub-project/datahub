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
