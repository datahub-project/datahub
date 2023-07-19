import { useGetSubscriptionQuery } from '../../../graphql/subscriptions.generated';

type Props = {
    isPersonal: boolean;
    entityUrn: string;
    groupUrn?: string;
};
const useSubscription = ({ isPersonal, entityUrn, groupUrn }: Props) => {
    const { data: getSubscriptionData, refetch: refetchSubscription } = useGetSubscriptionQuery({
        variables: {
            input: {
                entityUrn,
                groupUrn: groupUrn || undefined,
            },
        },
    });

    const subscription = isPersonal || groupUrn ? getSubscriptionData?.getSubscription ?? undefined : undefined;
    const isSubscribed = !!subscription;

    return { subscription, isSubscribed, refetchSubscription };
};

export default useSubscription;
