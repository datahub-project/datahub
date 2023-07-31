import { useGetSubscriptionQuery } from '../../../graphql/subscriptions.generated';

type Props = {
    isPersonal: boolean;
    entityUrn: string;
    groupUrn?: string;
};
const useSubscription = ({ isPersonal, entityUrn, groupUrn }: Props) => {
    const skip = !isPersonal && !groupUrn;
    const { data: getSubscriptionData, refetch: refetchSubscription } = useGetSubscriptionQuery({
        skip,
        variables: {
            input: {
                entityUrn,
                groupUrn: groupUrn || undefined,
            },
        },
    });

    const subscription = (!skip && getSubscriptionData?.getSubscription) || undefined;
    const isSubscribed = !!subscription;

    return { subscription, isSubscribed, refetchSubscription };
};

export default useSubscription;
