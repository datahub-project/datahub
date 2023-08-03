import { useGetSubscriptionQuery } from '../../../graphql/subscriptions.generated';

type Props = {
    isPersonal: boolean;
    entityUrn: string;
    groupUrn?: string;
};
const useSubscription = ({ isPersonal, entityUrn, groupUrn }: Props) => {
    const skip = !isPersonal && !groupUrn;
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

    const subscription = (!skip && getSubscriptionData?.getSubscription.subscription) || undefined;
    const isSubscribed = !!subscription;
    const canManageSubscription = loading ? null : getSubscriptionData?.getSubscription.privileges?.canManageEntity;

    return { subscription, isSubscribed, refetchSubscription, canManageSubscription };
};

export default useSubscription;
