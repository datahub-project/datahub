import { useMemo } from 'react';

import { useListDomainsQuery } from '@graphql/domain.generated';

export default function useDomains(parentDomainUrns: string | undefined, start: number, count: number, skip: boolean) {
    const { data, loading } = useListDomainsQuery({
        variables: {
            input: {
                start,
                count,
                parentDomain: parentDomainUrns,
            },
        },
        skip,
    });

    const domains = useMemo(() => {
        if (skip) return [];
        return data?.listDomains?.domains;
    }, [data, skip]);

    const total = useMemo(() => data?.listDomains?.total, [data]);

    return { data, domains, total, loading };
}
