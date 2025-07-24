import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListSubscriptionsQuery } from '@graphql/subscriptions.generated';
import { CorpUser, EntityType } from '@types';

const MAX_ASSETS_TO_FETCH = 50;

// TODO: Add Group subscriptions here as well.
export const useGetAssetsYouSubscribeTo = (user?: CorpUser | null, count = MAX_ASSETS_TO_FETCH) => {
    const { loading, data, error } = useListSubscriptionsQuery({
        variables: {
            input: {
                start: 0,
                count,
            },
        },
        skip: !user?.urn,
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const originEntities = data?.listSubscriptions?.subscriptions?.map((result) => result.entity) || [];

    const entities =
        originEntities.map((subscription) =>
            entityRegistry.getGenericEntityProperties(subscription.type as EntityType, subscription),
        ) || [];
    const total = data?.listSubscriptions?.total || 0;

    return { originEntities, entities, loading, error, total };
};
