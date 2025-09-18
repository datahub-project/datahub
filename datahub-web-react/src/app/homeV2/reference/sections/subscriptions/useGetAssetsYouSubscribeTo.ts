import { WatchQueryFetchPolicy } from '@apollo/client';
import { useCallback, useMemo } from 'react';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListSubscriptionsQuery } from '@graphql/subscriptions.generated';
import { CorpUser, Entity, EntityType } from '@types';

const MAX_ASSETS_TO_FETCH = 50;

interface Props {
    user?: CorpUser | null;
    initialCount?: number;
    fetchPolicy?: WatchQueryFetchPolicy;
    onCompleted?: () => void;
}

// TODO: Add Group subscriptions here as well.
export function useGetAssetsYouSubscribeTo({
    user,
    initialCount = MAX_ASSETS_TO_FETCH,
    fetchPolicy,
    onCompleted,
}: Props) {
    const getVariables = useCallback(
        (start: number, count: number) => ({
            input: {
                start,
                count,
            },
        }),
        [],
    );
    const { loading, data, error, refetch } = useListSubscriptionsQuery({
        variables: getVariables(0, initialCount),
        skip: !user?.urn,
        fetchPolicy: fetchPolicy ?? 'cache-first',
        nextFetchPolicy: 'cache-first',
        onCompleted,
    });

    const entityRegistry = useEntityRegistry();
    const originEntities = useMemo(
        () => data?.listSubscriptions?.subscriptions?.map((result) => result.entity) || [],
        [data?.listSubscriptions?.subscriptions],
    );

    const entities = originEntities.map((subscription) =>
        entityRegistry.getGenericEntityProperties(subscription.type as EntityType, subscription),
    );
    const total = data?.listSubscriptions?.total || 0;

    // For fetching paginated subscriptions based on start and count
    const fetchSubscriptions = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (start === 0) {
                return originEntities;
            }

            const result = await refetch(getVariables(start, count));

            return result.data?.listSubscriptions?.subscriptions?.map((res) => res.entity) || [];
        },
        [refetch, getVariables, originEntities],
    );

    return { originEntities, entities, loading, error, total, fetchSubscriptions };
}
