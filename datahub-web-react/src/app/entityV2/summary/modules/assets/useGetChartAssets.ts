import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { GetChartQuery } from '@graphql/chart.generated';
import { Entity, EntityType } from '@types';

export const useGetChartAssets = () => {
    const { urn, entityType, loading: entityLoading } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const chart = useBaseEntity<GetChartQuery>()?.chart;

    const originEntities = useMemo(() => {
        if (entityType !== EntityType.Chart) {
            return [];
        }
        return (chart?.dashboards?.relationships?.map((relationship) => relationship.entity).filter(Boolean) ||
            []) as Entity[];
    }, [chart?.dashboards?.relationships, entityType]);

    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            return originEntities.slice(start, start + count);
        },
        [originEntities],
    );

    const navigateToAssetsTab = () => {
        history.push(
            getEntityPath(
                entityType,
                urn,
                entityRegistry,
                false,
                false,
                entityRegistry.getCollectionName(EntityType.Dashboard),
            ),
        );
    };

    return {
        originEntities,
        loading: entityType === EntityType.Chart ? entityLoading : false,
        total: originEntities.length,
        fetchAssets,
        navigateToAssetsTab,
    };
};
