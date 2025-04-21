import _ from 'lodash';
import { Key } from 'react';

import { checkIsAssetLevelAssertionSubscription } from '@app/shared/subscribe/drawer/section/utils';
import { useDrawerState } from '@app/shared/subscribe/drawer/state/context';
import {
    createSubscriptionFunction,
    getEntityChangeTypesFromCheckedKeys,
    removeNestedTypeNames,
    updateSubscriptionFunction,
} from '@app/shared/subscribe/drawer/utils';

import { useCreateSubscriptionMutation, useUpdateSubscriptionMutation } from '@graphql/subscriptions.generated';
import {
    Assertion,
    DataHubSubscription,
    EntityChangeDetails,
    EntityChangeType,
    EntityType,
    NotificationSettingsInput,
    SubscriptionType,
} from '@types';

type UseUpsertSubScriptionParams = {
    entityUrn: string;
    entityType: EntityType;
    isSubscribed: boolean;
    groupUrn?: string;
    subscription?: DataHubSubscription;
    onCreateSuccess?: () => void;
    onRefetch?: () => void;
    forSubResource?: {
        assertion?: Assertion;
    };
};

/**
 * For assertion subscription forms.
 * Generates EntityChangeDetails based on the current state of the form...
 * ...and the current state of the subscription aspect.
 */
const generateEntityChangeDetailsFromAssertionSubscriptionForm = (
    forAssertion: Assertion,
    selectedEntityChangeTypes: EntityChangeType[],
    maybeExistingEntityChangeDetails?: EntityChangeDetails[],
): EntityChangeDetails[] => {
    // 1. Add this assertion's urn from to the filters for the selected types
    // Ie. lets say the user has checked the 'Passing' type. Then we should add forAssertion.urn to the filter.includeAssertions list
    const entityChangeDetails: EntityChangeDetails[] = selectedEntityChangeTypes.map((entityChangeType) => {
        const maybeExistingEntityTypeDetails = maybeExistingEntityChangeDetails?.find(
            (details) => details.entityChangeType.valueOf() === entityChangeType.valueOf(),
        );

        // If the existing subscription for this type is at the entity level, we make no changes.
        const isSubscribedAtEntityLevel =
            maybeExistingEntityTypeDetails && checkIsAssetLevelAssertionSubscription(maybeExistingEntityTypeDetails);
        if (isSubscribedAtEntityLevel) {
            return maybeExistingEntityTypeDetails;
        }

        // Else, we add this assertion's urn to the inclusive filter.
        const details: EntityChangeDetails = maybeExistingEntityTypeDetails ?? {
            entityChangeType,
        };
        details.filter = details.filter ?? {};
        details.filter.includeAssertions = details.filter.includeAssertions ?? [];

        details.filter.includeAssertions.push(forAssertion.urn);

        return details;
    });

    // 2.1 Remove this assertion's urn from the types that were unselected
    // 2.2 And, coalesce all the other subscriptions that are already on the entity.
    // NOTE: The return value of this method wil overwrite entityChangeTypes on the asset-level subscription object. So, we coalesce...
    // ...existing entity change types in to make sure we don't drop any types unrelated to this assertion in particular.
    if (maybeExistingEntityChangeDetails) {
        const existingChangeTypesThatAreNotSelectedInView: EntityChangeDetails[] = maybeExistingEntityChangeDetails
            .filter((details) => !selectedEntityChangeTypes.includes(details.entityChangeType))
            .map((details) => {
                const detailsCopy: EntityChangeDetails = _.cloneDeep(details);
                // Remove this urn from filters if it was unselected in this subresource's view
                if (detailsCopy.filter?.includeAssertions?.includes(forAssertion.urn)) {
                    detailsCopy.filter.includeAssertions = detailsCopy.filter.includeAssertions.filter(
                        (urn) => urn !== forAssertion.urn,
                    );
                    // If there was a filter for this change type & this sub resource is the last item in the filter, we can just remove the change type completely
                    if (!detailsCopy.filter.includeAssertions.length) {
                        return undefined;
                    }
                }
                return detailsCopy;
            })
            .filter((exists) => exists)
            .map((exists) => exists!);
        entityChangeDetails.push(...existingChangeTypesThatAreNotSelectedInView);
    }
    return entityChangeDetails;
};

/**
 * For top level entity subscription forms.
 * Generates EntityChangeDetails based on the current state of the form...
 * ...and the current state of the subscription aspect.
 */
const generateEntityChangeDetailsFromEntitySubscriptionForm = (
    selectedEntityChangeTypes: EntityChangeType[],
    keysWithAllFilteringCleared: Key[],
    maybeExistingEntityChangeDetails?: EntityChangeDetails[],
): EntityChangeDetails[] => {
    return selectedEntityChangeTypes.map((entityChangeType) => {
        const maybeExistingEntityTypeDetails = maybeExistingEntityChangeDetails?.find(
            (details) => details.entityChangeType.valueOf() === entityChangeType.valueOf(),
        );
        const details: EntityChangeDetails = maybeExistingEntityTypeDetails ?? {
            entityChangeType,
        };
        if (keysWithAllFilteringCleared.includes(entityChangeType) && details.filter) {
            delete details.filter;
        }
        return details;
    });
};

/**
 * Merges the state of the subscription form into the current state of the Subscription aspect,
 * and returns the target state that EntityChangeDetails should look like on the Subscription aspect
 * @param checkedKeys: subscription form state
 * @param keysWithAllFilteringCleared: subscription form state
 * @param forSubResource: subscription form state
 * @param subscription: current Subscription aspect stored in backend
 * @returns {EntityChangeDetails[]}
 */
const getEntityChangeDetailsFromSubscriptionFormState = (
    checkedKeys: Key[],
    keysWithAllFilteringCleared: Key[],
    forSubResource: UseUpsertSubScriptionParams['forSubResource'], // passed through so we re-use the parent hook's
    subscription?: DataHubSubscription,
): EntityChangeDetails[] => {
    const selectedEntityChangeTypes: EntityChangeType[] = getEntityChangeTypesFromCheckedKeys(checkedKeys);
    const existingEntityChangeDetails: EntityChangeDetails[] | undefined = subscription?.entityChangeTypes.map(
        (details) =>
            // remove '__typename' so this object can be re-used in gql request
            removeNestedTypeNames(details),
    );

    if (forSubResource?.assertion) {
        return generateEntityChangeDetailsFromAssertionSubscriptionForm(
            forSubResource.assertion,
            selectedEntityChangeTypes,
            existingEntityChangeDetails,
        );
    }
    return generateEntityChangeDetailsFromEntitySubscriptionForm(
        selectedEntityChangeTypes,
        keysWithAllFilteringCleared,
        existingEntityChangeDetails,
    );
};

const useUpsertSubscription = ({
    entityUrn,
    entityType,
    isSubscribed,
    groupUrn,
    subscription,
    onCreateSuccess,
    onRefetch,
    forSubResource,
}: UseUpsertSubScriptionParams) => {
    const { theme } = useCustomTheme();

    const {
        isPersonal,
        notificationTypes: { checkedKeys, keysWithAllFilteringCleared },
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

    const entityChangeDetails: EntityChangeDetails[] = getEntityChangeDetailsFromSubscriptionFormState(
        checkedKeys,
        keysWithAllFilteringCleared,
        forSubResource,
        subscription,
    );

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
            entityChangeTypes: entityChangeDetails,
            notificationSettings,
            onSuccess: onCreateSuccess,
            onRefetch,
            theme,
        });
    };

    const onUpdateSubscription = () => {
        updateSubscriptionFunction({
            updateSubscription,
            isPersonal,
            entityType,
            subscription,
            subscriptionTypes,
            entityChangeTypes: entityChangeDetails,
            notificationSettings,
            onRefetch,
            theme,
        });
    };

    return isSubscribed ? onUpdateSubscription : onCreateSubscription;
};

export default useUpsertSubscription;
