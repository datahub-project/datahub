import { useEffect, useState } from 'react';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntitySubscriptionSummaryQuery } from '@graphql/subscriptions.generated';
import { EntityType } from '@types';

type Props = {
    entityUrn: string;
    isEntityExists?: boolean;
};

const DEFAULT_SUBSCRIPTION_COUNT = 100;
const DEFAULT_USER_COUNT = 50;
const DEFAULT_GROUP_COUNT = 5;

const useSubscriptionSummary = ({ entityUrn, isEntityExists = true }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [isUserSubscribed, setIsUserSubscribed] = useState(false);
    const [subscribedUsersToFetch, setSubscribedUsersToFetch] = useState<number>(DEFAULT_USER_COUNT);
    const [subscribedGroupsToFetch, setSubscribedGroupsToFetch] = useState<number>(DEFAULT_GROUP_COUNT);
    const skip = !isEntityExists;

    const {
        data: entitySubscriptionSummaryData,
        refetch: refetchSubscriptionSummary,
        loading: isFetchingSubscriptionSummary,
    } = useGetEntitySubscriptionSummaryQuery({
        skip,
        variables: {
            input: {
                entityUrn,
                subscriptionCount: DEFAULT_SUBSCRIPTION_COUNT,
                numSubscribedUsers: subscribedUsersToFetch,
                numSubscribedGroups: subscribedGroupsToFetch,
            },
        },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        setIsUserSubscribed(entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed || false);
    }, [entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed]);

    const subscribedUsers = entitySubscriptionSummaryData?.getEntitySubscriptionSummary.subscribedUsers || [];
    const subscribedGroups = entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.subscribedGroups || [];

    // Maxes out at DEFAULT_SUBSCRIPTION_COUNT
    const numUserSubscriptions = entitySubscriptionSummaryData?.getEntitySubscriptionSummary.userSubscriptionCount || 0;
    // Maxes out at DEFAULT_SUBSCRIPTION_COUNT
    const numGroupSubscriptions =
        entitySubscriptionSummaryData?.getEntitySubscriptionSummary.groupSubscriptionCount || 0;
    const groupNames: string[] =
        (entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.subscribedGroups
            .map((group) => entityRegistry.getDisplayName(EntityType.CorpGroup, group))
            .filter((name) => !!name) as string[]) || [];

    const fetchMoreUsers =
        subscribedUsersToFetch < numUserSubscriptions
            ? () => {
                  setSubscribedUsersToFetch((prev) => prev + DEFAULT_USER_COUNT);
              }
            : null;

    const fetchMoreGroups =
        subscribedGroupsToFetch < numGroupSubscriptions
            ? () => {
                  setSubscribedGroupsToFetch((prev) => prev + DEFAULT_GROUP_COUNT);
              }
            : null;

    return {
        isUserSubscribed,
        numUserSubscriptions,
        numGroupSubscriptions,
        groupNames,
        setIsUserSubscribed,
        refetchSubscriptionSummary,
        isFetchingSubscriptionSummary,
        subscribedGroups,
        subscribedUsers,
        fetchMoreGroups,
        fetchMoreUsers,
    };
};

export default useSubscriptionSummary;
