import { useMemo } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

export default function useDomainsByUrns(urns: string[]) {
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.Domain],
                filters: [{ field: 'urn', condition: FilterOperator.Equal, values: urns }],
                count: urns.length,
            },
        },
        skip: !urns.length,
        fetchPolicy: 'cache-and-network',
    });

    const domains = useMemo(() => {
        if (urns.length === 0) return [];
        if (data === undefined) return undefined;

        return (data.searchAcrossEntities?.searchResults ?? []).map((result) => result.entity).filter(isDomain);
    }, [data, urns]);

    return {
        data,
        domains,
        loading,
    };
}
