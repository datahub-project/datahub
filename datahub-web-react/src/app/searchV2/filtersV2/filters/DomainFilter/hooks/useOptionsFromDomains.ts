<<<<<<< HEAD
import { useMemo } from 'react';

import { domainKeyAccessor, extractParentDomains } from '@app/searchV2/filtersV2/filters/DomainFilter/utils';
import { getUniqueItemsByKeyFromArrrays } from '@app/searchV2/filtersV2/utils';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { Domain } from '@src/types.generated';
=======
import { Domain } from '@src/types.generated';
import { useMemo } from 'react';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { domainKeyAccessor, extractParentDomains } from '../utils';
import { getUniqueItemsByKeyFromArrrays } from '../../../utils';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function useOptionsFromDomains(domains: Domain[]): NestedSelectOption[] {
    return useMemo(() => {
        const parentDomains = extractParentDomains(domains);
        const parentUrns = parentDomains.map(domainKeyAccessor);
        const finalDomains = getUniqueItemsByKeyFromArrrays([domains, parentDomains], domainKeyAccessor);

        return finalDomains.map((domain) => ({
            value: domain.urn,
            label: domain.properties?.name ?? domain.urn,
            entity: domain,
            parentValue: domain.parentDomains?.domains?.[0]?.urn,
            isParent: parentUrns.includes(domain.urn),
        }));
    }, [domains]);
}
