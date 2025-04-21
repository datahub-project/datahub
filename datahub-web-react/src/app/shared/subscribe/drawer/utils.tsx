import { CheckCircleFilled, QuestionCircleOutlined } from '@ant-design/icons';
import { Tooltip, colors } from '@components';
import { Typography, message, notification } from 'antd';
import { DataNode } from 'antd/lib/tree';
import _ from 'lodash';
import React, { Key } from 'react';
import styled from 'styled-components/macro';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { ActorTypes } from '@app/settings/personal/notifications/constants';

import {
    useCreateSubscriptionMutation,
    useDeleteSubscriptionMutation,
    useUpdateSubscriptionMutation,
} from '@graphql/subscriptions.generated';
import {
    Assertion,
    DataHubSubscription,
    EmailNotificationSettings,
    EntityChangeDetails,
    EntityChangeType,
    EntityType,
    NotificationSettingsInput,
    SlackNotificationSettings,
    SubscriptionType,
} from '@types';

const REFETCH_DELAY = 3000;

const NotificationTypeText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    margin-right: 8px;
`;

const NotificationTypeDetailsText = styled(NotificationTypeText)`
    font-size: 12px;
    color: #999;
`;

const TooltipIcon = styled(QuestionCircleOutlined)`
    margin-left: 4px;
    font-size: 12px;
`;

const ASSERTION_NODE_KEY = 'assertion_changes';
const INCIDENTS_NODE_KEY = 'incident_changes';
const DEPRECATION_NODE_KEY = EntityChangeType.Deprecated;
const SCHEMA_NODE_KEY = 'schema_changes';
const OWNERSHIP_CHANGE_NODE_KEY = 'ownership_change';
const GLOSSARY_TERM_CHANGE_NODE_KEY = 'glossary_term_change';
const TAG_CHANGE_NODE_KEY = 'tag_change';

const NESTED_NODE_KEY_PARENTS = new Set([
    SCHEMA_NODE_KEY,
    OWNERSHIP_CHANGE_NODE_KEY,
    GLOSSARY_TERM_CHANGE_NODE_KEY,
    TAG_CHANGE_NODE_KEY,
    ASSERTION_NODE_KEY,
    INCIDENTS_NODE_KEY,
]);

export const getEntityChangeTypesFromCheckedKeys = (checkedKeys: Key[]): EntityChangeType[] => {
    return checkedKeys.filter((key) => !NESTED_NODE_KEY_PARENTS.has(key as string)) as EntityChangeType[];
};

export const ASSERTION_SUBSCRIPTION_RELATED_ENTITY_CHANGE_TYPES = [
    EntityChangeType.AssertionFailed,
    EntityChangeType.AssertionPassed,
    EntityChangeType.AssertionError,
];

const getDetailsLabelForAssertionTypeChange = (
    assertionChangeType:
        | EntityChangeType.AssertionPassed
        | EntityChangeType.AssertionFailed
        | EntityChangeType.AssertionError,
    subscription?: DataHubSubscription,
    keysWithFilteringCleared?: Key[],
    isForAssertionSubscription?: boolean,
): string => {
    const maybeAssertionSubscription = subscription?.entityChangeTypes.find(
        (details) => details.entityChangeType === assertionChangeType,
    );
    const maybeAssertionFilters = maybeAssertionSubscription?.filter?.includeAssertions;
    const assertionFiltersCount = maybeAssertionFilters?.length;
    const hasDisabledAssertionFiltering = keysWithFilteringCleared?.includes(assertionChangeType.valueOf());

    let label = '';
    if (!isForAssertionSubscription && assertionFiltersCount && !hasDisabledAssertionFiltering) {
        label = `(subscribed to ${assertionFiltersCount} individual assertions)`;
    } else if (isForAssertionSubscription && maybeAssertionSubscription && !maybeAssertionFilters) {
        label = 'All assertions on asset selected';
    }
    return label;
};

const getAssertionsNode = (maybeContext?: NodeRenderingContext): DataNode => {
    const { subscription, forSubResource, keysWithFilteringCleared } = maybeContext ?? {};

    const isForAssertionSubscription = !!forSubResource?.assertion;
    const assertionFailedDetailsLabel = getDetailsLabelForAssertionTypeChange(
        EntityChangeType.AssertionFailed,
        subscription,
        keysWithFilteringCleared,
        isForAssertionSubscription,
    );

    const assertionPassedDetailsLabel = getDetailsLabelForAssertionTypeChange(
        EntityChangeType.AssertionPassed,
        subscription,
        keysWithFilteringCleared,
        isForAssertionSubscription,
    );

    const assertionErrorDetailsLabel = getDetailsLabelForAssertionTypeChange(
        EntityChangeType.AssertionError,
        subscription,
        keysWithFilteringCleared,
        isForAssertionSubscription,
    );

    return {
        key: ASSERTION_NODE_KEY,
        title: <NotificationTypeText>Assertion status</NotificationTypeText>,
        children: [
            {
                key: EntityChangeType.AssertionFailed,
                title: (
                    <NotificationTypeText>
                        {isForAssertionSubscription ? 'This' : 'Any'} assertion fails{' '}
                        <NotificationTypeDetailsText>{assertionFailedDetailsLabel}</NotificationTypeDetailsText>
                    </NotificationTypeText>
                ),
            },
            {
                key: EntityChangeType.AssertionPassed,
                title: (
                    <NotificationTypeText>
                        {isForAssertionSubscription ? 'This' : 'Any'} assertion passes{' '}
                        <NotificationTypeDetailsText>{assertionPassedDetailsLabel}</NotificationTypeDetailsText>
                    </NotificationTypeText>
                ),
            },
            {
                key: EntityChangeType.AssertionError,
                title: (
                    <NotificationTypeText>
                        {isForAssertionSubscription ? 'This' : 'Any'} assertion errors{' '}
                        <NotificationTypeDetailsText>{assertionErrorDetailsLabel}</NotificationTypeDetailsText>
                    </NotificationTypeText>
                ),
            },
        ],
    };
};

const incidentsNode: DataNode = {
    key: INCIDENTS_NODE_KEY,
    title: <NotificationTypeText>Incident status changes</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.IncidentRaised,
            title: <NotificationTypeText>An incident is raised</NotificationTypeText>,
        },
        {
            key: EntityChangeType.IncidentResolved,
            title: <NotificationTypeText>An incident is resolved</NotificationTypeText>,
        },
    ],
};

const deprecationNode: DataNode = {
    key: DEPRECATION_NODE_KEY,
    title: <NotificationTypeText>Entity has been deprecated</NotificationTypeText>,
};

// TODO: in V2 add documentation changes notifications
// const documentationNode: DataNode = {
//     key: DOCUMENTATION_NODE_KEY,
//     title: <NotificationTypeText>Documentation changes</NotificationTypeText>,
// };

const schemaNode: DataNode = {
    key: SCHEMA_NODE_KEY,
    title: <NotificationTypeText>Schema change events</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.OperationColumnAdded,
            title: <NotificationTypeText>A column is added</NotificationTypeText>,
        },
        {
            key: EntityChangeType.OperationColumnRemoved,
            title: <NotificationTypeText>A column is removed</NotificationTypeText>,
        },
        {
            key: EntityChangeType.OperationColumnModified,
            title: (
                <NotificationTypeText>
                    A column is modified
                    <Tooltip title="Receive notifications when a column is renamed or its type is changed">
                        <TooltipIcon />
                    </Tooltip>
                </NotificationTypeText>
            ),
        },
    ],
};

// TODO: in V2 add row-based operation metadata notifications
// const operationalMetadataNode: DataNode = {
//     key: OPERATIONAL_METADATA_NODE_KEY,
//     title: <NotificationTypeText>Operational change events</NotificationTypeText>,
//     children: [
//         {
//             key: EntityChangeType.OperationRowsInserted,
//             title: <NotificationTypeText>Rows are inserted</NotificationTypeText>,
//         },
//         {
//             key: EntityChangeType.OperationRowsUpdated,
//             title: <NotificationTypeText>Rows are updated</NotificationTypeText>,
//         },
//         {
//             key: EntityChangeType.OperationRowsRemoved,
//             title: <NotificationTypeText>Rows are removed</NotificationTypeText>,
//         },
//     ],
// };

const ownershipChangeNode: DataNode = {
    key: OWNERSHIP_CHANGE_NODE_KEY,
    title: <NotificationTypeText>Ownership changes</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.OwnerAdded,
            title: <NotificationTypeText>An owner is added</NotificationTypeText>,
        },
        {
            key: EntityChangeType.OwnerRemoved,
            title: <NotificationTypeText>An owner is removed</NotificationTypeText>,
        },
    ],
};

const glossaryTermChangeNode: DataNode = {
    key: GLOSSARY_TERM_CHANGE_NODE_KEY,
    title: <NotificationTypeText>Glossary term changes</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.GlossaryTermAdded,
            title: <NotificationTypeText>A glossary term is added</NotificationTypeText>,
        },
        {
            key: EntityChangeType.GlossaryTermRemoved,
            title: <NotificationTypeText>A glossary term is removed</NotificationTypeText>,
        },
        {
            key: EntityChangeType.GlossaryTermProposed,
            title: (
                <NotificationTypeText>
                    Glossary term proposal changes
                    <Tooltip title="Someone has proposed or rejected a new glossary term on this entity">
                        <TooltipIcon />
                    </Tooltip>
                </NotificationTypeText>
            ),
        },
    ],
};

const tagChangeNode: DataNode = {
    key: TAG_CHANGE_NODE_KEY,
    title: <NotificationTypeText>Tag changes</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.TagAdded,
            title: <NotificationTypeText>A tag is added</NotificationTypeText>,
        },
        {
            key: EntityChangeType.TagRemoved,
            title: <NotificationTypeText>A tag is removed</NotificationTypeText>,
        },
        {
            key: EntityChangeType.TagProposed,
            title: (
                <NotificationTypeText>
                    Tag proposal changes
                    <Tooltip title="Someone has proposed or rejected a new tag on this entity">
                        <TooltipIcon />
                    </Tooltip>
                </NotificationTypeText>
            ),
        },
    ],
};

type NodeRenderingContext = {
    subscription?: DataHubSubscription;
    forSubResource?: { assertion?: Assertion };
    keysWithFilteringCleared: Array<Key>;
};

export const getTreeDataForEntity = (entityType: string, maybeContext?: NodeRenderingContext): DataNode[] => {
    switch (entityType) {
        case EntityType.Dataset:
            if (maybeContext?.forSubResource?.assertion) {
                return [getAssertionsNode(maybeContext)];
            }
            return [
                deprecationNode,
                getAssertionsNode(maybeContext),
                incidentsNode,
                schemaNode,
                ownershipChangeNode,
                glossaryTermChangeNode,
                tagChangeNode,
            ];
        default:
            return [deprecationNode, ownershipChangeNode, glossaryTermChangeNode, tagChangeNode];
    }
};

export const deleteSubscriptionFunction = ({
    subscription,
    isPersonal,
    deleteSubscription,
    onSuccess,
    onRefetch,
    theme,
}: {
    subscription: DataHubSubscription;
    isPersonal: boolean;
    deleteSubscription: ReturnType<typeof useDeleteSubscriptionMutation>[0];
    onSuccess?: () => void;
    onRefetch?: () => void;
    theme?: Theme;
}) => {
    deleteSubscription({
        variables: {
            input: { subscriptionUrn: subscription.subscriptionUrn },
        },
        fetchPolicy: 'no-cache',
    })
        .then(() => {
            onSuccess?.();
            analytics.event({
                type: EventType.SubscriptionDeleteSuccessEvent,
                subscriptionUrn: subscription.subscriptionUrn,
                entityUrn: subscription.entity.urn,
                entityType: subscription.entity.type,
                entityChangeTypes: subscription.entityChangeTypes.map((changeType) => changeType.entityChangeType),
                actorType: isPersonal ? ActorTypes.PERSONAL : ActorTypes.GROUP,
                sinkTypes: subscription.notificationConfig?.notificationSettings?.sinkTypes ?? [],
            });
            const description = isPersonal
                ? 'You have unsubscribed from this entity.'
                : 'You have unsubscribed your group from this entity.';
            notification.success({
                message: `Success`,
                description,
                placement: 'bottomLeft',
                duration: 3,
                icon: <CheckCircleFilled style={{ color: getColor('primary', 500, theme) }} />,
            });
            if (onRefetch) window.setTimeout(onRefetch, REFETCH_DELAY);
        })
        .catch((e: unknown) => {
            analytics.event({
                type: EventType.SubscriptionDeleteErrorEvent,
                subscriptionUrn: subscription.subscriptionUrn,
                entityUrn: subscription.entity.urn,
                entityType: subscription.entity.type,
                entityChangeTypes: subscription.entityChangeTypes.map((changeType) => changeType.entityChangeType),
                actorType: isPersonal ? ActorTypes.PERSONAL : ActorTypes.GROUP,
                sinkTypes: subscription.notificationConfig?.notificationSettings?.sinkTypes ?? [],
            });
            message.destroy();
            if (e instanceof Error) {
                message.error({
                    content: `Failed to delete subscription`,
                    duration: 3,
                });
            }
        });
};

export const createSubscriptionFunction = ({
    createSubscription,
    isPersonal,
    entityType,
    groupUrn,
    entityUrn,
    subscriptionTypes,
    entityChangeTypes,
    notificationSettings,
    onSuccess,
    onRefetch,
    theme,
}: {
    createSubscription: ReturnType<typeof useCreateSubscriptionMutation>[0];
    isPersonal: boolean;
    entityType: EntityType;
    groupUrn: string | undefined;
    entityUrn: string;
    subscriptionTypes: Array<SubscriptionType>;
    entityChangeTypes: Array<EntityChangeDetails>;
    notificationSettings: NotificationSettingsInput;
    onSuccess?: () => void;
    onRefetch?: () => void;
    theme?: Theme;
}) => {
    const input = {
        groupUrn,
        entityUrn,
        subscriptionTypes,
        entityChangeTypes,
        notificationConfig: {
            notificationSettings,
        },
    };

    createSubscription({
        variables: {
            input,
        },
        fetchPolicy: 'no-cache',
    })
        .then((result) => {
            onSuccess?.();
            analytics.event({
                type: EventType.SubscriptionCreateSuccessEvent,
                subscriptionUrn: result.data?.createSubscription.subscriptionUrn ?? '',
                entityUrn,
                entityType,
                entityChangeTypes: entityChangeTypes.map((details) => details.entityChangeType),
                sinkTypes: notificationSettings?.sinkTypes || [],
                actorType: isPersonal ? ActorTypes.PERSONAL : ActorTypes.GROUP,
            });
            const description = isPersonal
                ? 'You are now subscribed to this entity.'
                : 'Your group is now subscribed to this entity.';
            notification.success({
                message: 'Success',
                description,
                placement: 'bottomLeft',
                duration: 3,
                icon: <CheckCircleFilled style={{ color: getColor('primary', 500, theme) }} />,
            });
            if (onRefetch) window.setTimeout(onRefetch, REFETCH_DELAY);
        })
        .catch((e: unknown) => {
            analytics.event({
                type: EventType.SubscriptionCreateErrorEvent,
                entityUrn,
                entityType,
                entityChangeTypes: entityChangeTypes.map((details) => details.entityChangeType),
                sinkTypes: notificationSettings.sinkTypes || [],
                actorType: isPersonal ? ActorTypes.PERSONAL : ActorTypes.GROUP,
            });
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to create subscription`, duration: 3 });
            }
        });
};

/**
 * Remove the GraphQL __typename fields from any object
 * @param obj
 * @returns {T}
 */
export function removeNestedTypeNames<T>(obj: T): T {
    // Check if the argument is an object and not null
    if (typeof obj !== 'object' || obj === null) return obj;
    // Shallow clone object so we're not modifying it directly
    // NOTE: the recursive call below will effectively do a deep clone where necessary
    const clonedObj = _.clone(obj as object);

    // Iterate over object properties
    Object.keys(clonedObj).forEach((prop) => {
        if (prop === '__typename') {
            // Remove __typename property
            delete clonedObj[prop];
        } else if (typeof clonedObj[prop] === 'object' && clonedObj[prop] !== null) {
            // Recurse for nested objects and arrays
            clonedObj[prop] = removeNestedTypeNames(clonedObj[prop]);
        }
    });
    return clonedObj as T;
}

export function cleanAssertionDescription(builderStateData) {
    const newBuilderStateData = { ...builderStateData };
    // Create a shallow copy of the assertion to avoid modifying the original object
    const assertion = { ...newBuilderStateData.assertion };
    // Description support not added in backend for testAssertion api, so deleting it here won't reflect in the original assertion
    delete assertion.description;
    newBuilderStateData.assertion = assertion;
    const { type } = assertion;
    return { newBuilderStateData, type };
}

export const updateSubscriptionFunction = ({
    updateSubscription,
    isPersonal,
    entityType,
    subscription,
    subscriptionTypes,
    entityChangeTypes,
    notificationSettings,
    onRefetch,
    theme,
}: {
    updateSubscription: ReturnType<typeof useUpdateSubscriptionMutation>[0];
    isPersonal: boolean;
    entityType: EntityType;
    subscription: DataHubSubscription | undefined;
    subscriptionTypes: Array<SubscriptionType>;
    entityChangeTypes: Array<EntityChangeDetails>;
    notificationSettings: NotificationSettingsInput;
    onRefetch?: () => void;
    theme?: Theme;
}) => {
    const entityChangeTypesAdded = _.difference(
        entityChangeTypes.map((details) => details.entityChangeType),
        subscription?.entityChangeTypes.map((details) => details.entityChangeType) ?? [],
    );
    const entityChangeTypesRemoved = _.difference(
        subscription?.entityChangeTypes.map((changeType) => changeType.entityChangeType) ?? [],
        entityChangeTypes.map((details) => details.entityChangeType),
    );

    const sinkTypesAdded = _.difference(
        notificationSettings.sinkTypes,
        subscription?.notificationConfig?.notificationSettings?.sinkTypes ?? [],
    );
    const sinkTypesRemoved = _.difference(
        subscription?.notificationConfig?.notificationSettings?.sinkTypes ?? [],
        notificationSettings.sinkTypes || [],
    );

    if (subscription && subscription.subscriptionUrn) {
        const input = {
            subscriptionUrn: subscription?.subscriptionUrn,
            subscriptionTypes,
            entityChangeTypes,
            notificationConfig: {
                notificationSettings,
            },
        };

        updateSubscription({
            variables: {
                input,
            },
            fetchPolicy: 'no-cache',
        })
            .then(() => {
                analytics.event({
                    type: EventType.SubscriptionUpdateSuccessEvent,
                    subscriptionUrn: subscription.subscriptionUrn,
                    entityUrn: subscription.entity.urn,
                    entityType,
                    entityChangeTypes: entityChangeTypes.map((details) => details.entityChangeType),
                    entityChangeTypesAdded,
                    entityChangeTypesRemoved,
                    sinkTypes: notificationSettings.sinkTypes || [],
                    sinkTypesAdded,
                    sinkTypesRemoved,
                    actorType: isPersonal ? ActorTypes.PERSONAL : ActorTypes.GROUP,
                });
                const description = isPersonal
                    ? 'You have updated your subscription to this entity.'
                    : 'You have updated the subscription to this entity for your group.';
                notification.success({
                    message: `Success`,
                    description,
                    placement: 'bottomLeft',
                    duration: 3,
                    icon: <CheckCircleFilled style={{ color: getColor('primary', 500, theme) }} />,
                });
                if (onRefetch) window.setTimeout(onRefetch, REFETCH_DELAY);
            })
            .catch((e: unknown) => {
                analytics.event({
                    type: EventType.SubscriptionUpdateErrorEvent,
                    subscriptionUrn: subscription.subscriptionUrn,
                    entityUrn: subscription.entity.urn,
                    entityType,
                    entityChangeTypes: entityChangeTypes.map((details) => details.entityChangeType),
                    sinkTypes: notificationSettings.sinkTypes || [],
                    actorType: isPersonal ? ActorTypes.PERSONAL : ActorTypes.GROUP,
                });
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to update subscription`,
                        duration: 3,
                    });
                }
            });
    }
};

export const getSinkTypesForSubscription = (subscription?: DataHubSubscription) => {
    return subscription?.notificationConfig?.notificationSettings?.sinkTypes || [];
};

export const getSlackSubscriptionChannel = (isPersonal: boolean, subscription?: DataHubSubscription) => {
    const subUserHandle =
        subscription?.notificationConfig?.notificationSettings?.slackSettings?.userHandle || undefined;
    const subChannels = subscription?.notificationConfig?.notificationSettings?.slackSettings?.channels;
    const subGroupChannel = subChannels?.length ? subChannels[0] : undefined;
    return isPersonal ? subUserHandle : subGroupChannel;
};

export const getSlackSettingsChannel = (isPersonal: boolean, settings?: SlackNotificationSettings) => {
    const settingsUserHandle = settings?.userHandle || undefined;
    const settingsChannels = settings?.channels;
    const settingsGroupChannel = settingsChannels?.length ? settingsChannels[0] : undefined;
    return isPersonal ? settingsUserHandle : settingsGroupChannel;
};

export const getEmailSubscriptionChannel = (__: boolean, subscription?: DataHubSubscription) => {
    return subscription?.notificationConfig?.notificationSettings?.emailSettings?.email;
};

export const getEmailSettingsChannel = (__: boolean, settings?: EmailNotificationSettings) => {
    return settings?.email;
};
