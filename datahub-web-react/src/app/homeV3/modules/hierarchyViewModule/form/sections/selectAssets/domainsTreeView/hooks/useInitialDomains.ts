import { useMemo } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

export default function useInitialDomains(domainUrns: string[]) {
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.Domain],
                filters: [{ field: 'urn', condition: FilterOperator.Equal, values: domainUrns }],
                count: domainUrns.length,
            },
        },
        skip: !domainUrns.length,
        fetchPolicy: 'cache-and-network',
    });

    const domains = useMemo(() => {
        if (domainUrns.length === 0) return [];
        if (data === undefined) return undefined;

        return (data.searchAcrossEntities?.searchResults ?? []).map((result) => result.entity).filter(isDomain);
    }, [data, domainUrns]);

    return {
        data,
        domains,
        loading,
    };
}
