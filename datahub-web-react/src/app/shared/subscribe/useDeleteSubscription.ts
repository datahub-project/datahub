import { useDeleteSubscriptionMutation } from '../../../graphql/subscriptions.generated';
import { deleteSubscriptionFunction } from './drawer/utils';

type Props = {
    subscriptionUrn?: string;
    onRefetch?: () => void;
};

const useDeleteSubscription = ({ subscriptionUrn, onRefetch }: Props) => {
    const [deleteSubscription] = useDeleteSubscriptionMutation();

    return () => {
        if (subscriptionUrn) deleteSubscriptionFunction({ subscriptionUrn, deleteSubscription, onRefetch });
    };
};

export default useDeleteSubscription;
