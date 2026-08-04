import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
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

export const useGetDashboardAssets = () => {
    const { urn, entityType, loading: entityLoading } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const dashboard = useBaseEntity<GetDashboardQuery>()?.dashboard;

    // Same sources as legacy Related Assets: Contents (charts) + Data Sources (chart upstreams).
    const charts = useMemo(() => {
        if (entityType !== EntityType.Dashboard) {
            return [] as Entity[];
        }
        return (dashboard?.charts?.relationships?.map((relationship) => relationship.entity).filter(Boolean) ||
            []) as Entity[];
    }, [dashboard?.charts?.relationships, entityType]);

    const chartUpstreamDatasetUrns = useMemo(() => {
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
    }, [charts]);

    const { data: hydratedDataSources, loading: searchLoading } = useGetSearchResultsQuery({
        skip: entityType !== EntityType.Dashboard || chartUpstreamDatasetUrns.length === 0,
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

    const dataSources = useMemo(
        () =>
            (hydratedDataSources?.search?.searchResults?.map((result) => result.entity).filter(Boolean) ||
                []) as Entity[],
        [hydratedDataSources?.search?.searchResults],
    );

    const originEntities = useMemo(() => dedupeEntities([...charts, ...dataSources]), [charts, dataSources]);

    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            return originEntities.slice(start, start + count);
        },
        [originEntities],
    );

    const navigateToAssetsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Contents`);
    };

    const loading =
        entityType === EntityType.Dashboard
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
