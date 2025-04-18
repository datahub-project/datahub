import { Domain } from '@src/types.generated';
import { useMemo } from 'react';
import { getUniqueItemsByKeyFromArrrays } from '../../../utils';
import { domainKeyAccessor } from '../utils';

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
