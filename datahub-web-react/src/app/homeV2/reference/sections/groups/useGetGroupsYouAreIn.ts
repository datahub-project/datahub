import { useGetUserGroupsQuery } from '../../../../../graphql/user.generated';
import { CorpUser, EntityType } from '../../../../../types.generated';
import { useEntityRegistry } from '../../../../useEntityRegistry';

const MAX_ASSETS_TO_FETCH = 50;

// TODO: Add Group subscriptions here as well.
export const useGetGroupsYouAreIn = (user?: CorpUser | null, count = MAX_ASSETS_TO_FETCH) => {
    const { loading, data, error } = useGetUserGroupsQuery({
        variables: {
            urn: user?.urn as string,
            start: 0,
            count,
        },
        skip: !user?.urn,
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const entities =
        data?.corpUser?.relationships?.relationships
            ?.filter((relationship) => relationship.entity)
            .map((relationship) =>
                entityRegistry.getGenericEntityProperties(
                    (relationship.entity as any).type as EntityType,
                    relationship.entity,
                ),
            ) || [];

    return { entities, loading, error };
};
