import { useGetEntitySubscriptionSummaryQuery } from '../../../graphql/subscriptions.generated';
import { CorpGroup } from '../../../types.generated';
import { getGroupName } from '../../settings/personal/utils';

type Props = {
    entityUrn: string;
};

const useSubscriptionSummary = ({ entityUrn }: Props) => {
    const { data: entitySubscriptionSummaryData, refetch: refetchSubscriptionSummary } =
        useGetEntitySubscriptionSummaryQuery({
            variables: {
                input: {
                    entityUrn,
                },
            },
        });

    const isUserSubscribed = entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed || false;
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
        refetchSubscriptionSummary,
    };
};

export default useSubscriptionSummary;
