import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { GetChartQuery } from '@graphql/chart.generated';
import { Entity, EntityType } from '@types';

function dedupeEntities(entities: (Entity | null | undefined)[]): Entity[] {
    const seen = new Set<string>();
    const result: Entity[] = [];
    entities.forEach((entity) => {
        if (entity && !seen.has(entity.urn)) {
            seen.add(entity.urn);
            result.push(entity);
        }
    });
    return result;
}

export const useGetChartAssets = () => {
    const { urn, entityType, loading: entityLoading } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const chart = useBaseEntity<GetChartQuery>()?.chart;

    const originEntities = useMemo(() => {
        if (entityType !== EntityType.Chart) {
            return [];
        }
        const dataSources =
            chart?.inputs?.relationships
                ?.map((relationship) => relationship.entity)
                ?.filter((entity) => entity?.__typename === 'Dataset') || [];
        const dashboards =
            chart?.dashboards?.relationships?.map((relationship) => relationship.entity).filter(Boolean) || [];
        return dedupeEntities([...dataSources, ...dashboards]);
    }, [chart?.dashboards?.relationships, chart?.inputs?.relationships, entityType]);

    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            return originEntities.slice(start, start + count);
        },
        [originEntities],
    );

    const navigateToAssetsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Lineage`);
    };

    return {
        originEntities,
        loading: entityType === EntityType.Chart ? entityLoading : false,
        total: originEntities.length,
        fetchAssets,
        navigateToAssetsTab,
    };
};
