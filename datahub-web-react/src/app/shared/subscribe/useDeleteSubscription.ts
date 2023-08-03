import { useDeleteSubscriptionMutation } from '../../../graphql/subscriptions.generated';
import { DataHubSubscription } from '../../../types.generated';
import { deleteSubscriptionFunction } from './drawer/utils';

type Props = {
    subscription?: DataHubSubscription;
    isPersonal: boolean;
    onDeleteSuccess?: () => void;
    onRefetch?: () => void;
};

const useDeleteSubscription = ({ subscription, isPersonal, onDeleteSuccess, onRefetch }: Props) => {
    const [deleteSubscription] = useDeleteSubscriptionMutation();

    return () => {
        if (subscription?.subscriptionUrn)
            deleteSubscriptionFunction({
                subscription,
                isPersonal,
                deleteSubscription,
                onSuccess: onDeleteSuccess,
                onRefetch,
            });
    };
};

export default useDeleteSubscription;
