import {
    useCreateSubscriptionMutation,
    useUpdateSubscriptionMutation,
} from '../../../../graphql/subscriptions.generated';
import {
    DataHubSubscription,
    EntityType,
    NotificationSettingsInput,
    SubscriptionType,
} from '../../../../types.generated';
import { useDrawerState } from './state/context';
import { createSubscriptionFunction, getEntityChangeTypesFromCheckedKeys, updateSubscriptionFunction } from './utils';

type Props = {
    entityUrn: string;
    entityType: EntityType;
    isSubscribed: boolean;
    groupUrn?: string;
    subscription?: DataHubSubscription;
    onCreateSuccess?: () => void;
    onRefetch?: () => void;
};

const useUpsertSubscription = ({
    entityUrn,
    entityType,
    isSubscribed,
    groupUrn,
    subscription,
    onCreateSuccess,
    onRefetch,
}: Props) => {
    const {
        isPersonal,
        notificationTypes: { checkedKeys },
        subscribeToUpstream,
        notificationSinkTypes,
        slack: {
            subscription: { channel: slackChannel, saveAsDefault: slackSaveAsDefault },
        },
        email: {
            subscription: { channel: emailChannel, saveAsDefault: emailSaveAsDefault },
        },
    } = useDrawerState();

    const [createSubscription] = useCreateSubscriptionMutation();
    const [updateSubscription] = useUpdateSubscriptionMutation();

    const entityChangeTypes = getEntityChangeTypesFromCheckedKeys(checkedKeys);

    const subscriptionTypes = subscribeToUpstream
        ? [SubscriptionType.EntityChange, SubscriptionType.UpstreamEntityChange]
        : [SubscriptionType.EntityChange];

    const notificationSettings: NotificationSettingsInput = {
        sinkTypes: notificationSinkTypes,
        slackSettings: {
            userHandle: !slackSaveAsDefault && isPersonal && slackChannel ? slackChannel : undefined,
            channels: !slackSaveAsDefault && !isPersonal && slackChannel ? [slackChannel] : undefined,
        },
        emailSettings:
            !emailSaveAsDefault && emailChannel
                ? {
                    email: emailChannel,
                }
                : undefined,
    };

    const onCreateSubscription = () => {
        createSubscriptionFunction({
            createSubscription,
            isPersonal,
            entityType,
            groupUrn: groupUrn || undefined,
            entityUrn,
            subscriptionTypes,
            entityChangeTypes,
            notificationSettings,
            onSuccess: onCreateSuccess,
            onRefetch,
        });
    };

    const onUpdateSubscription = () => {
        updateSubscriptionFunction({
            updateSubscription,
            isPersonal,
            entityType,
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
