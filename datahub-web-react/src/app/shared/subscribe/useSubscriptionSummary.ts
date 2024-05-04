import { useEffect, useState } from 'react';
import { useGetEntitySubscriptionSummaryQuery } from '../../../graphql/subscriptions.generated';
import { EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    entityUrn: string;
    isEntityExists?: boolean;
};

const useSubscriptionSummary = ({ entityUrn, isEntityExists = true }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [isUserSubscribed, setIsUserSubscribed] = useState(false);
    const skip = !isEntityExists;
    const { data: entitySubscriptionSummaryData, refetch: refetchSubscriptionSummary } =
        useGetEntitySubscriptionSummaryQuery({
            skip,
            variables: {
                input: {
                    entityUrn,
                },
            },
        });

    useEffect(() => {
        setIsUserSubscribed(entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed || false);
    }, [entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed]);

    const numUserSubscriptions = entitySubscriptionSummaryData?.getEntitySubscriptionSummary.userSubscriptionCount || 0;
    // Maxes out at 100 by default.
    const numGroupSubscriptions =
        entitySubscriptionSummaryData?.getEntitySubscriptionSummary.groupSubscriptionCount || 0;
    const groupNames: string[] =
        (entitySubscriptionSummaryData?.getEntitySubscriptionSummary.exampleGroups
            .map((group) => entityRegistry.getDisplayName(EntityType.CorpGroup, group))
            .filter((name) => !!name) as string[]) || [];

    return {
        isUserSubscribed,
        numUserSubscriptions,
        numGroupSubscriptions,
        groupNames,
        setIsUserSubscribed,
        refetchSubscriptionSummary,
    };
};

export default useSubscriptionSummary;
