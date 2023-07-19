import { useDeleteSubscriptionMutation } from '../../../graphql/subscriptions.generated';
import { deleteSubscriptionFunction } from './drawer/utils';

type Props = {
    subscriptionUrn?: string;
    onSuccess: () => void;
};

const useDeleteSubscription = ({ subscriptionUrn, onSuccess }: Props) => {
    const [deleteSubscription] = useDeleteSubscriptionMutation();

    return () => {
        console.log('delete', subscriptionUrn);
        if (subscriptionUrn) deleteSubscriptionFunction(subscriptionUrn, deleteSubscription, onSuccess);
    };
};

export default useDeleteSubscription;
