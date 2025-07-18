import { useMemo } from 'react';

import { useListDomainsQuery } from '@graphql/domain.generated';

export default function useDomains(parentDomainUrns?: string) {
    const { data, loading } = useListDomainsQuery({
        variables: {
            input: {
                start: 0,
                count: 1000,
                parentDomain: parentDomainUrns,
            },
        },
    });

    const domains = useMemo(() => {
        if (data === undefined) return undefined;

        return data.listDomains?.domains ?? [];
    }, [data]);

    return { data, domains, loading };
}
