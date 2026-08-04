import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { GetDashboardQuery } from '@graphql/dashboard.generated';
import { Entity, EntityType } from '@types';

function dedupeEntities(entities: Entity[]): Entity[] {
    const seen = new Set<string>();
    const result: Entity[] = [];
    entities.forEach((entity) => {
        if (!seen.has(entity.urn)) {
            seen.add(entity.urn);
            result.push(entity);
        }
    });
    return result;
}

export const useGetDashboardAssets = () => {
    const { urn, entityType, loading: entityLoading } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const dashboard = useBaseEntity<GetDashboardQuery>()?.dashboard;

    const originEntities = useMemo(() => {
        if (entityType !== EntityType.Dashboard) {
            return [];
        }
        const charts = (dashboard?.charts?.relationships?.map((relationship) => relationship.entity).filter(Boolean) ||
            []) as Entity[];
        const datasets = (dashboard?.datasets?.relationships
            ?.map((relationship) => relationship.entity)
            .filter(Boolean) || []) as Entity[];
        return dedupeEntities([...charts, ...datasets]);
    }, [dashboard?.charts?.relationships, dashboard?.datasets?.relationships, entityType]);

    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            return originEntities.slice(start, start + count);
        },
        [originEntities],
    );

    const navigateToAssetsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Contents`);
    };

    return {
        originEntities,
        loading: entityType === EntityType.Dashboard ? entityLoading : false,
        total: originEntities.length,
        fetchAssets,
        navigateToAssetsTab,
    };
};
