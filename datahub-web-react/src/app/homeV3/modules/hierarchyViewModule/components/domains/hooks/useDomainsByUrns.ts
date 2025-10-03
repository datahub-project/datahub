import { useMemo } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';

import { useGetEntitiesQuery } from '@graphql/entity.generated';

export default function useDomainsByUrns(urns: string[]) {
    const { isReloading, onReloadingFinished } = useModuleContext();

    const { data, loading } = useGetEntitiesQuery({
        variables: {
            urns,
            checkForExistence: true,
        },
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        nextFetchPolicy: 'cache-first',
        skip: urns.length === 0,
        onCompleted: () => onReloadingFinished(),
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
