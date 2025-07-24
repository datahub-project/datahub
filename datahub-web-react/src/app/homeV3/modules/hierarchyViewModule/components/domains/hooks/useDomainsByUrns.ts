import { useMemo } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';

import { useGetEntitiesQuery } from '@graphql/entity.generated';

export default function useDomainsByUrns(urns: string[]) {
    const { data, loading } = useGetEntitiesQuery({
        variables: {
            urns,
        },
        fetchPolicy: 'cache-and-network',
        skip: urns.length === 0,
    });

    const domains = useMemo(() => {
        if (urns.length === 0) return [];
        if (data === undefined) return undefined;

        return (data.entities ?? []).filter(isDomain);
    }, [data, urns]);

    return {
        data,
        domains,
        loading,
    };
}
