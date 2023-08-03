import { useEffect, useState } from 'react';
import { useGetEntitySubscriptionSummaryQuery } from '../../../graphql/subscriptions.generated';
import { CorpGroup } from '../../../types.generated';
import { getGroupName } from '../../settings/personal/utils';

type Props = {
    entityUrn: string;
};

const useSubscriptionSummary = ({ entityUrn }: Props) => {
    const [isUserSubscribed, setIsUserSubscribed] = useState(false);
    const { data: entitySubscriptionSummaryData, refetch: refetchSubscriptionSummary } =
        useGetEntitySubscriptionSummaryQuery({
            variables: {
                input: {
                    entityUrn,
                },
            },
        });

    useEffect(() => {
        setIsUserSubscribed(entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed || false);
    }, [entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed]);

    const numUserSubscriptions = entitySubscriptionSummaryData?.getEntitySubscriptionSummary.numUserSubscriptions || 0;
    // Maxes out at 100 by default.
    const numGroupSubscriptions =
        entitySubscriptionSummaryData?.getEntitySubscriptionSummary.numGroupSubscriptions || 0;
    const groupNames: string[] =
        (entitySubscriptionSummaryData?.getEntitySubscriptionSummary.topGroups
            .map((group) => getGroupName(group as CorpGroup))
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
