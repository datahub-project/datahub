import { Domain } from '@src/types.generated';
import React, { useMemo } from 'react';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { domainKeyAccessor, extractParentDomains } from '../utils';
import { getUniqueItemsByKeyFromArrrays } from '../../../utils';

export default function useOptionsFromDomains(
    domains: Domain[],
    render: (domain: Domain) => React.ReactNode,
): SelectOption[] {
    return useMemo(() => {
        const parentDomains = extractParentDomains(domains);
        const parentUrns = parentDomains.map(domainKeyAccessor);
        const finalDomains = getUniqueItemsByKeyFromArrrays([domains, parentDomains], domainKeyAccessor);

        return finalDomains.map((domain) => ({
            value: domain.urn,
            label: render(domain),
            entity: domain,
            parentValue: domain.parentDomains?.domains?.[0]?.urn,
            isParent: parentUrns.includes(domain.urn),
        }));
    }, [domains, render]);
}
