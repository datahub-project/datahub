import { Domain } from '@src/types.generated';
import { useMemo } from 'react';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { domainKeyAccessor, extractParentDomains } from '../utils';
import { getUniqueItemsByKeyFromArrrays } from '../../../utils';

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
