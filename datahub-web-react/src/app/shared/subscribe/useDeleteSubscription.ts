import { useDeleteSubscriptionMutation } from '../../../graphql/subscriptions.generated';
import { DataHubSubscription } from '../../../types.generated';
import { deleteSubscriptionFunction } from './drawer/utils';

type Props = {
    subscription?: DataHubSubscription;
    isPersonal: boolean;
    onRefetch?: () => void;
};

const useDeleteSubscription = ({ subscription, isPersonal, onRefetch }: Props) => {
    const [deleteSubscription] = useDeleteSubscriptionMutation();

    return () => {
        if (subscription?.subscriptionUrn)
            deleteSubscriptionFunction({ subscription, isPersonal, deleteSubscription, onRefetch });
    };
};

export default useDeleteSubscription;
