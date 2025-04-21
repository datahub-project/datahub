import { Key } from 'react';

import { useDrawerState } from '@app/shared/subscribe/drawer/state/context';
import {
    ASSERTION_SUBSCRIPTION_RELATED_ENTITY_CHANGE_TYPES,
    removeNestedTypeNames,
    updateSubscriptionFunction,
} from '@app/shared/subscribe/drawer/utils';

import { useGetDatasetAssertionsWithMonitorsQuery } from '@graphql/monitor.generated';
import { useUpdateSubscriptionMutation } from '@graphql/subscriptions.generated';
import {
    Assertion,
    DataHubSubscription,
    EntityChangeDetails,
    EntityChangeType,
    EntityType,
    NotificationSettingsInput,
    SubscriptionType,
} from '@types';

export function checkIsKeyBeingToggledForAssertionSubscriptionsAtAssetLevel(
    key: Key,
    subscription?: DataHubSubscription,
): boolean {
    // 1. Get assertion subscriptions that are made at the asset level
    const assetLevelAssertionSubscriptions = subscription?.entityChangeTypes?.filter(
        checkIsAssetLevelAssertionSubscription,
    );

    // 2. If we are currently unchecking 'all assertions', then we need to ensure that
    // none of the assertion subscriptions are at an asset level
    const isThisKeyBatchModifyingSubscriptions = key === 'assertion_changes';
    const hasAnyAssetLevelAssertionSubscriptions = !!assetLevelAssertionSubscriptions?.length;

    // 3. If we are currently unchecking a specific assertion change type, then we need to ensure that
    // we aren't currently subscribed to that change type at an asset level
    const isThisKeyDirectlyUncheckingAssetLevelSubscription = !!assetLevelAssertionSubscriptions?.find(
        (details) => details.entityChangeType.valueOf() === String(key),
    );
    const isThisKeyBatchUncheckingSubscriptionsAtAssetLevel =
        isThisKeyBatchModifyingSubscriptions && hasAnyAssetLevelAssertionSubscriptions;

    // 4. Return if either case 2. or 3. are true
    return isThisKeyDirectlyUncheckingAssetLevelSubscription || isThisKeyBatchUncheckingSubscriptionsAtAssetLevel;
}

export function checkIsAssetLevelAssertionSubscription(details: EntityChangeDetails): boolean {
    return (
        ASSERTION_SUBSCRIPTION_RELATED_ENTITY_CHANGE_TYPES.includes(details.entityChangeType) &&
        // the absence of an inclusive filter indicates an entity-level subscription
        !details.filter?.includeAssertions
    );
}

type UseRemoveAssertionFromAssetLevelSubscriptionParams = {
    entityUrn: string;
    entityType: EntityType;
    onRefetch?: () => void;
};

/**
 * If the user is subscribed to all assertions at the asset level for a change type,
 * then call this to unsubscribe the user from a specific assertion.
 * NOTE: this will essentially add all assertions on the asset EXCEPT for the {@param assertionToUnsubscribe}
 * into an includeAssertions filter. Thereby switching the user's subscription to an assertion-level instead of asset-level.
 * NOTE: the side effect of this is that the user will no longer be auto-subscribed to newly created assertions on this asset
 */
export function useRemoveAssertionFromAssetLevelSubscription({
    entityUrn,
    entityType,
    onRefetch,
}: UseRemoveAssertionFromAssetLevelSubscriptionParams) {
    const { theme } = useCustomTheme();
    const { data } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn: entityUrn },
        fetchPolicy: 'cache-first',
    });
    const allAssetAssertionUrns: string[] | undefined = data?.dataset?.assertions?.assertions.map(
        (assertion) => assertion.urn,
    );

    const {
        isPersonal,
        notificationSinkTypes,
        slack: {
            subscription: { channel: slackChannel, saveAsDefault: slackSaveAsDefault },
        },
        email: {
            subscription: { channel: emailChannel, saveAsDefault: emailSaveAsDefault },
        },
    } = useDrawerState();

    const [updateSubscription] = useUpdateSubscriptionMutation();

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

    const onRemoveAssertionFromAssetLevelSubscription = (
        assertionToUnsubscribe: Assertion,
        entityChangeTypeToUnsubscribeFrom: EntityChangeType,
        subscriptionToUpdate: DataHubSubscription,
    ) => {
        // Validate we have necessary data to continue this action...
        if (!allAssetAssertionUrns?.length) {
            throw new Error('No assertions found.');
        }
        const entityChangeDetailToUpdate = subscriptionToUpdate.entityChangeTypes.find(
            (details) => details.entityChangeType === entityChangeTypeToUnsubscribeFrom,
        );
        if (!entityChangeDetailToUpdate) {
            throw new Error(
                `Could not find existing subscription for change type ${entityChangeTypeToUnsubscribeFrom}`,
            );
        }

        // Add an includeAssertions filter to this EntityChangeType which consists of all assertions except this one...
        const entityChangeTypeIncludeAssertionsFilter = allAssetAssertionUrns.filter(
            (urn) => urn !== assertionToUnsubscribe.urn,
        );
        const entityChangeDetails: EntityChangeDetails[] = subscriptionToUpdate.entityChangeTypes.map((details) => {
            if (details.entityChangeType !== entityChangeTypeToUnsubscribeFrom) {
                return details;
            }
            const newDetails = { ...details };
            newDetails.filter = newDetails.filter ?? {};
            newDetails.filter.includeAssertions = entityChangeTypeIncludeAssertionsFilter;
            return newDetails;
        });

        // Update the subscription
        updateSubscriptionFunction({
            updateSubscription,
            isPersonal,
            entityType,
            subscription: subscriptionToUpdate,
            subscriptionTypes: [SubscriptionType.EntityChange],
            entityChangeTypes: removeNestedTypeNames(entityChangeDetails),
            notificationSettings,
            onRefetch,
            theme,
        });
    };

    return onRemoveAssertionFromAssetLevelSubscription;
}

/**
 * Converts a string name of {@link enum EntityChangeType} to enum
 * @param name ie ASSERTION_PASSED
 * @returns {@enum EntityChangeType}
 */
export function getEntityChangeTypeWithName(name: string): EntityChangeType | undefined {
    const entityChangeTypeIndex = Object.values(EntityChangeType).indexOf(name as any);
    return EntityChangeType[Object.keys(EntityChangeType)[entityChangeTypeIndex]];
}
