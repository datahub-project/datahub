import { useGetSubscriptionQuery } from '@graphql/subscriptions.generated';
import { DataHubSubscription } from '@types';

type Props = {
    isPersonal: boolean;
    entityUrn: string;
    groupUrn?: string;
    isEntityExists?: boolean;
};

const useSubscription = ({ isPersonal, entityUrn, groupUrn, isEntityExists = true }: Props) => {
    const skip = (!isPersonal && !groupUrn) || !isEntityExists;

    const {
        data: getSubscriptionData,
        loading,
        refetch: refetchSubscription,
    } = useGetSubscriptionQuery({
        skip,
        variables: {
            input: {
                entityUrn,
                groupUrn: groupUrn || undefined,
            },
        },
    });

    const subscription = (!skip && getSubscriptionData?.getSubscription.subscription) as
        | DataHubSubscription
        | undefined;
    const isSubscribed = !!subscription;
    const canManageSubscription = loading ? null : getSubscriptionData?.getSubscription?.privileges?.canManageEntity;

    return { subscription, isSubscribed, refetchSubscription, canManageSubscription };
};

export default useSubscription;
