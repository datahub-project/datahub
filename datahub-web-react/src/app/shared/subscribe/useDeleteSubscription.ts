import { deleteSubscriptionFunction } from '@app/shared/subscribe/drawer/utils';
import { useCustomTheme } from '@src/customThemeContext';

import { useDeleteSubscriptionMutation } from '@graphql/subscriptions.generated';
import { DataHubSubscription } from '@types';

type Props = {
    subscription?: DataHubSubscription;
    onDeleteSuccess?: () => void;
    onRefetch?: () => void;
};

const useDeleteSubscription = ({ subscription, onDeleteSuccess, onRefetch }: Props) => {
    const [deleteSubscription] = useDeleteSubscriptionMutation();
    const { theme } = useCustomTheme();

    return () => {
        if (subscription?.subscriptionUrn)
            deleteSubscriptionFunction({
                subscription,
                deleteSubscription,
                onSuccess: onDeleteSuccess,
                onRefetch,
                theme,
            });
    };
};

export default useDeleteSubscription;
