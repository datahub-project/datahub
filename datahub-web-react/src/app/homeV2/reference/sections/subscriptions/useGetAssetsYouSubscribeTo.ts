import { useListSubscriptionsQuery } from '../../../../../graphql/subscriptions.generated';
import { CorpUser, EntityType } from '../../../../../types.generated';
import { useEntityRegistry } from '../../../../useEntityRegistry';

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
    const entities =
        data?.listSubscriptions?.subscriptions
            ?.filter((subscription) => subscription.entity)
            .map((subscription) =>
                entityRegistry.getGenericEntityProperties(subscription.entity.type as EntityType, subscription.entity),
            ) || [];
    const total = data?.listSubscriptions?.total || 0;

    return { entities, loading, error, total };
};
