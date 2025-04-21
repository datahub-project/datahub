import { deleteSubscriptionFunction } from '@app/shared/subscribe/drawer/utils';

import { useDeleteSubscriptionMutation } from '@graphql/subscriptions.generated';
import { DataHubSubscription } from '@types';

type Props = {
    subscription?: DataHubSubscription;
    isPersonal: boolean;
    onDeleteSuccess?: () => void;
    onRefetch?: () => void;
};

const useDeleteSubscription = ({ subscription, isPersonal, onDeleteSuccess, onRefetch }: Props) => {
    const [deleteSubscription] = useDeleteSubscriptionMutation();
    const { theme } = useCustomTheme();

    return () => {
        if (subscription?.subscriptionUrn)
            deleteSubscriptionFunction({
                subscription,
                isPersonal,
                deleteSubscription,
                onSuccess: onDeleteSuccess,
                onRefetch,
                theme,
            });
    };
};

export default useDeleteSubscription;
