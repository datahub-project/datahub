import { useMemo } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator, SortOrder } from '@types';

export default function useDomains(parentDomainUrn: string | undefined, start: number, count: number, skip: boolean) {
    const { data, loading, refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                start,
                count,
                query: '*',
                types: [EntityType.Domain],
                orFilters: parentDomainUrn
                    ? [{ and: [{ field: 'parentDomain', values: [parentDomainUrn] }] }]
                    : [{ and: [{ field: 'parentDomain', condition: FilterOperator.Exists, negated: true }] }],
                sortInput: {
                    sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
                },
                searchFlags: {
                    skipCache: true,
                },
            },
        },
        // FYI: this request could be called manually by some user action (e.g. expanding)
        // to keep it simple network is used for first call (possible longer first loading of modules with automatic expanding of child nodes)
        fetchPolicy: 'cache-and-network',
        nextFetchPolicy: 'cache-first',
        skip,
    });

    const domains = useMemo(() => {
        if (skip) return [];
        return data?.searchAcrossEntities?.searchResults.map((result) => result.entity).filter(isDomain);
    }, [data, skip]);

    const total = useMemo(() => data?.searchAcrossEntities?.total, [data]);

    return { data, domains, total, loading, refetch };
}
