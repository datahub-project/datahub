import {
    useCreateSubscriptionMutation,
    useUpdateSubscriptionMutation,
} from '../../../../graphql/subscriptions.generated';
import { DataHubSubscription, NotificationSettingsInput, SubscriptionType } from '../../../../types.generated';
import { useDrawerState } from './state/context';
import { createSubscriptionFunction, getEntityChangeTypesFromCheckedKeys, updateSubscriptionFunction } from './utils';

type Props = {
    entityUrn: string;
    isSubscribed: boolean;
    groupUrn?: string;
    subscription?: DataHubSubscription;
    onRefetch?: () => void;
};

const useUpsertSubscription = ({ entityUrn, isSubscribed, groupUrn, subscription, onRefetch }: Props) => {
    const {
        isPersonal,
        notificationTypes: { checkedKeys },
        subscribeToUpstream,
        notificationSinkTypes,
        slack: {
            subscription: { channel, saveAsDefault },
        },
    } = useDrawerState();

    const [createSubscription] = useCreateSubscriptionMutation();
    const [updateSubscription] = useUpdateSubscriptionMutation();

    const entityChangeTypes = getEntityChangeTypesFromCheckedKeys(checkedKeys);

    const subscriptionTypes = subscribeToUpstream
        ? [SubscriptionType.EntityChange, SubscriptionType.UpstreamEntityChange]
        : [SubscriptionType.EntityChange];

    const notificationSettings: NotificationSettingsInput | undefined = {
        sinkTypes: notificationSinkTypes,
        slackSettings: {
            userHandle: !saveAsDefault && isPersonal && channel ? channel : undefined,
            channels: !saveAsDefault && !isPersonal && channel ? [channel] : undefined,
        },
    };

    const onCreateSubscription = () => {
        createSubscriptionFunction({
            createSubscription,
            groupUrn: groupUrn || undefined,
            entityUrn,
            subscriptionTypes,
            entityChangeTypes,
            notificationSettings,
            onRefetch,
        });
    };

    const onUpdateSubscription = () => {
        updateSubscriptionFunction({
            updateSubscription,
            subscription,
            subscriptionTypes,
            entityChangeTypes,
            notificationSettings,
            onRefetch,
        });
    };

    return isSubscribed ? onUpdateSubscription : onCreateSubscription;
};

export default useUpsertSubscription;
