<<<<<<< HEAD
import { useMemo } from 'react';

import { domainKeyAccessor } from '@app/searchV2/filtersV2/filters/DomainFilter/utils';
import { getUniqueItemsByKeyFromArrrays } from '@app/searchV2/filtersV2/utils';
import { Domain } from '@src/types.generated';
=======
import { Domain } from '@src/types.generated';
import { useMemo } from 'react';
import { getUniqueItemsByKeyFromArrrays } from '../../../utils';
import { domainKeyAccessor } from '../utils';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
