import { useMemo } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

export default function useDomainsByUrns(urns: string[]) {
    const { isReloading, onReloadingFinished } = useModuleContext();

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                orFilters: [{ and: [{ field: 'urn', values: urns }] }],
            },
        },
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        nextFetchPolicy: 'cache-first',
        skip: urns.length === 0,
        onCompleted: () => onReloadingFinished(),
    });

    const domains = useMemo(() => {
        if (urns.length === 0) return [];
        if (data === undefined) return undefined;

        return (data.searchAcrossEntities?.searchResults?.map((r) => r.entity) ?? []).filter(isDomain);
    }, [data, urns]);

    return {
        data,
        domains,
        loading,
    };
}
