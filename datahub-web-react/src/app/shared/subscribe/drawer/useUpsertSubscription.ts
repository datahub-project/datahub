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
    onSuccess: () => void;
};

const useUpsertSubscription = ({ entityUrn, isSubscribed, groupUrn, subscription, onSuccess }: Props) => {
    const {
        // todo - this isn't a form value, it's just some global context we might want to pass around
        isPersonal,
        checkedKeys,
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

    // todo - add comments to unclear areas

    const notificationSettings: NotificationSettingsInput | undefined =
        channel && !saveAsDefault
            ? {
                  slackSettings: {
                      userHandle: isPersonal ? channel : undefined,
                      channels: isPersonal ? undefined : [channel],
                  },
              }
            : undefined;

    const onCreateSubscription = () => {
        createSubscriptionFunction(
            createSubscription,
            onSuccess,
            groupUrn || undefined,
            entityUrn,
            subscriptionTypes,
            entityChangeTypes,
            notificationSinkTypes,
            notificationSettings,
        );
    };

    const onUpdateSubscription = () => {
        updateSubscriptionFunction(
            updateSubscription,
            onSuccess,
            subscription,
            subscriptionTypes,
            entityChangeTypes,
            notificationSinkTypes,
            notificationSettings,
        );
    };

    return isSubscribed ? onUpdateSubscription : onCreateSubscription;
};

export default useUpsertSubscription;
