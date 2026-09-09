import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { GetDashboardQuery } from '@graphql/dashboard.generated';
import { useGetSearchResultsQuery } from '@graphql/search.generated';
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

type EntityWithUpstream = Entity & {
    upstream?: { relationships?: Array<{ entity?: Entity | null }> };
};

function useDashboardCharts(): Entity[] {
    const { entityType } = useEntityData();
    const dashboard = useBaseEntity<GetDashboardQuery>()?.dashboard;

    return useMemo(() => {
        if (entityType !== EntityType.Dashboard) {
            return [];
        }
        return (dashboard?.charts?.relationships?.map((relationship) => relationship.entity).filter(Boolean) ||
            []) as Entity[];
    }, [dashboard?.charts?.relationships, entityType]);
}

export const useGetDashboardContents = (skip = false) => {
    const { urn, entityType, loading: entityLoading } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const charts = useDashboardCharts();
    const originEntities = useMemo(() => (skip ? [] : charts), [charts, skip]);

    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            return originEntities.slice(start, start + count);
        },
        [originEntities],
    );

    const navigateToAssetsTab = () => {
        history.push(getEntityPath(entityType, urn, entityRegistry, false, false, 'Contents'));
    };

    return {
        originEntities,
        loading: !skip && entityType === EntityType.Dashboard ? entityLoading : false,
        total: originEntities.length,
        fetchAssets,
        navigateToAssetsTab,
    };
};

export const useGetDashboardDataSources = (skip = false) => {
    const { urn, entityType, loading: entityLoading } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const charts = useDashboardCharts();

    const chartUpstreamDatasetUrns = useMemo(() => {
        if (skip) {
            return [];
        }
        const urns = charts.flatMap((chart) => {
            const upstream = (chart as EntityWithUpstream).upstream?.relationships;
            return (
                upstream
                    ?.map((relationship) => relationship.entity)
                    .filter((entity): entity is Entity => entity?.type === EntityType.Dataset && !!entity.urn)
                    .map((entity) => entity.urn) || []
            );
        });
        return Array.from(new Set(urns));
    }, [charts, skip]);

    const { data: hydratedDataSources, loading: searchLoading } = useGetSearchResultsQuery({
        skip: skip || entityType !== EntityType.Dashboard || chartUpstreamDatasetUrns.length === 0,
        variables: {
            input: {
                type: EntityType.Dataset,
                query: '',
                filters: [
                    {
                        field: 'urn',
                        values: chartUpstreamDatasetUrns,
                    },
                ],
                count: chartUpstreamDatasetUrns.length,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const originEntities = useMemo(
        () =>
            dedupeEntities(
                (hydratedDataSources?.search?.searchResults?.map((result) => result.entity).filter(Boolean) ||
                    []) as Entity[],
            ),
        [hydratedDataSources?.search?.searchResults],
    );

    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            return originEntities.slice(start, start + count);
        },
        [originEntities],
    );

    const navigateToAssetsTab = () => {
        history.push(getEntityPath(entityType, urn, entityRegistry, false, false, 'Lineage'));
    };

    const loading =
        !skip && entityType === EntityType.Dashboard
            ? entityLoading || (chartUpstreamDatasetUrns.length > 0 && searchLoading)
            : false;

    return {
        originEntities,
        loading,
        total: originEntities.length,
        fetchAssets,
        navigateToAssetsTab,
    };
};
