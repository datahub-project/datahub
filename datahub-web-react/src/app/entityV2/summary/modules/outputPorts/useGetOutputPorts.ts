import { useCallback, useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { OUTPUT_PORTS_FIELD } from '@app/search/utils/constants';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const NUMBER_OF_OUTPUT_PORTS_TO_FETCH = 10;

export function useGetOutputPorts(initialCount = NUMBER_OF_OUTPUT_PORTS_TO_FETCH) {
    const { urn, entityType } = useEntityData();
    const { isReloading, onReloadingFinished } = useModuleContext();

    const getInputVariables = useCallback(
        (start: number, count: number) => ({
            urn,
            input: {
                query: '*',
                start,
                count,
                filters: [{ field: OUTPUT_PORTS_FIELD, value: 'true' }],
                searchFlags: { skipCache: true },
            },
        }),
        [urn],
    );

    const {
        loading: searchLoading,
        data,
        refetch,
    } = useListDataProductAssetsQuery({
        variables: getInputVariables(0, initialCount),
        skip: entityType !== EntityType.DataProduct,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        onCompleted: () => onReloadingFinished?.(),
    });

    const outputPorts = useMemo(
        () => data?.listDataProductAssets?.searchResults?.map((result) => result.entity) || [],
        [data?.listDataProductAssets?.searchResults],
    );
    const total = data?.listDataProductAssets?.total || 0;
    const loading = searchLoading || !data;

    const fetchOutputPorts = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (start === 0) {
                return outputPorts;
            }

            const result = await refetch(getInputVariables(start, count));

            return result.data?.listDataProductAssets?.searchResults?.map((res) => res.entity) || [];
        },
        [refetch, getInputVariables, outputPorts],
    );

    return { loading, total, fetchOutputPorts };
}
