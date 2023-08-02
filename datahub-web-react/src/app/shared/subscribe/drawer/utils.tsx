import React, { Key } from 'react';
import difference from 'lodash/difference';
import { Tooltip, Typography, message, notification } from 'antd';
import { DataNode } from 'antd/lib/tree';
import { CheckCircleFilled, QuestionCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import {
    DataHubSubscription,
    EntityChangeType,
    EntityType,
    NotificationSettingsInput,
    SubscriptionType,
} from '../../../../types.generated';
import {
    GetGroupNotificationSettingsQuery,
    GetUserNotificationSettingsQuery,
} from '../../../../graphql/settings.generated';
import {
    useCreateSubscriptionMutation,
    useDeleteSubscriptionMutation,
    useUpdateSubscriptionMutation,
} from '../../../../graphql/subscriptions.generated';
import analytics from '../../../analytics/analytics';
import { EventType } from '../../../analytics';
import { SubscriberTypes } from '../../../settings/personal/notifications/constants';

const REFETCH_DELAY = 3000;

const NotificationTypeText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    margin-right: 8px;
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

const assertionsNode: DataNode = {
    key: ASSERTION_NODE_KEY,
    title: <NotificationTypeText>Assertion status changes</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.AssertionFailed,
            title: <NotificationTypeText>Changes to failing</NotificationTypeText>,
        },
        {
            key: EntityChangeType.AssertionPassed,
            title: <NotificationTypeText>Changes to passing</NotificationTypeText>,
        },
    ],
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
                    A new glossary term is proposed
                    <Tooltip title="Someone has proposed adding a glossary term, but it has not beed added">
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
                    A new tag is proposed
                    <Tooltip title="Someone has proposed adding a tag, but it has not beed added">
                        <TooltipIcon />
                    </Tooltip>
                </NotificationTypeText>
            ),
        },
    ],
};

export const getTreeDataForEntity = (entityType: string): DataNode[] => {
    switch (entityType) {
        case EntityType.Dataset:
            return [
                deprecationNode,
                assertionsNode,
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
    onRefetch,
}: {
    subscription: DataHubSubscription;
    isPersonal: boolean;
    deleteSubscription: ReturnType<typeof useDeleteSubscriptionMutation>[0];
    onRefetch?: () => void;
}) => {
    deleteSubscription({
        variables: {
            input: { subscriptionUrn: subscription.subscriptionUrn },
        },
    })
        .then(() => {
            analytics.event({
                type: EventType.SubscriptionDeleteSuccessEvent,
                entityType: subscription.entity.type,
                entityChangeTypes: subscription.entityChangeTypes,
                subscriberType: isPersonal ? SubscriberTypes.PERSONAL : SubscriberTypes.GROUP,
                sinkTypes: subscription.notificationConfig?.notificationSettings?.sinkTypes ?? [],
            });
            notification.success({
                message: `Success`,
                description: 'You have unsubscribed from this entity.',
                placement: 'bottomLeft',
                duration: 3,
            });
            if (onRefetch) window.setTimeout(onRefetch, REFETCH_DELAY);
        })
        .catch((e: unknown) => {
            analytics.event({
                type: EventType.SubscriptionDeleteErrorEvent,
                entityType: subscription.entity.type,
                entityChangeTypes: subscription.entityChangeTypes,
                subscriberType: isPersonal ? SubscriberTypes.PERSONAL : SubscriberTypes.GROUP,
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
    onRefetch,
}: {
    createSubscription: ReturnType<typeof useCreateSubscriptionMutation>[0];
    isPersonal: boolean;
    entityType: EntityType;
    groupUrn: string | undefined;
    entityUrn: string;
    subscriptionTypes: Array<SubscriptionType>;
    entityChangeTypes: Array<EntityChangeType>;
    notificationSettings: NotificationSettingsInput;
    onRefetch?: () => void;
}) => {
    createSubscription({
        variables: {
            input: {
                groupUrn,
                entityUrn,
                subscriptionTypes,
                entityChangeTypes,
                notificationConfig: {
                    notificationSettings,
                },
            },
        },
    })
        .then(() => {
            analytics.event({
                type: EventType.SubscriptionCreateSuccessEvent,
                entityType,
                entityChangeTypes,
                sinkTypes: notificationSettings?.sinkTypes,
                subscriberType: isPersonal ? SubscriberTypes.PERSONAL : SubscriberTypes.GROUP,
            });
            notification.success({
                message: 'Success',
                description: 'You are now following changes on this entity.',
                placement: 'bottomLeft',
                duration: 3,
                icon: <CheckCircleFilled style={{ color: '#078781' }} />,
            });
            if (onRefetch) window.setTimeout(onRefetch, REFETCH_DELAY);
        })
        .catch((e: unknown) => {
            analytics.event({
                type: EventType.SubscriptionCreateErrorEvent,
                entityType,
                entityChangeTypes,
                sinkTypes: notificationSettings.sinkTypes,
                subscriberType: isPersonal ? SubscriberTypes.PERSONAL : SubscriberTypes.GROUP,
            });
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to create subscription`, duration: 3 });
            }
        });
};

export const updateSubscriptionFunction = ({
    updateSubscription,
    isPersonal,
    entityType,
    subscription,
    subscriptionTypes,
    entityChangeTypes,
    notificationSettings,
    onRefetch,
}: {
    updateSubscription: ReturnType<typeof useUpdateSubscriptionMutation>[0];
    isPersonal: boolean;
    entityType: EntityType;
    subscription: DataHubSubscription | undefined;
    subscriptionTypes: Array<SubscriptionType>;
    entityChangeTypes: Array<EntityChangeType>;
    notificationSettings: NotificationSettingsInput;
    onRefetch?: () => void;
}) => {
    const entityChangeTypesAdded = difference(entityChangeTypes, subscription?.entityChangeTypes ?? []);
    const entityChangeTypesRemoved = difference(subscription?.entityChangeTypes ?? [], entityChangeTypes);

    const sinkTypesAdded = difference(
        notificationSettings.sinkTypes,
        subscription?.notificationConfig?.notificationSettings?.sinkTypes ?? [],
    );
    const sinkTypesRemoved = difference(
        subscription?.notificationConfig?.notificationSettings?.sinkTypes ?? [],
        notificationSettings.sinkTypes,
    );

    if (subscription && subscription.subscriptionUrn) {
        updateSubscription({
            variables: {
                input: {
                    subscriptionUrn: subscription?.subscriptionUrn,
                    subscriptionTypes,
                    entityChangeTypes,
                    notificationConfig: {
                        notificationSettings,
                    },
                },
            },
        })
            .then(() => {
                analytics.event({
                    type: EventType.SubscriptionUpdateSuccessEvent,
                    entityType,
                    entityChangeTypes,
                    entityChangeTypesAdded,
                    entityChangeTypesRemoved,
                    sinkTypes: notificationSettings.sinkTypes,
                    sinkTypesAdded,
                    sinkTypesRemoved,
                    subscriberType: isPersonal ? SubscriberTypes.PERSONAL : SubscriberTypes.GROUP,
                });
                notification.success({
                    message: `Success`,
                    description: 'You have updated your subscription to this entity.',
                    placement: 'bottomLeft',
                    duration: 3,
                    icon: <CheckCircleFilled style={{ color: '#078781' }} />,
                });
                if (onRefetch) window.setTimeout(onRefetch, REFETCH_DELAY);
            })
            .catch((e: unknown) => {
                analytics.event({
                    type: EventType.SubscriptionUpdateErrorEvent,
                    entityType,
                    entityChangeTypes,
                    sinkTypes: notificationSettings.sinkTypes,
                    subscriberType: isPersonal ? SubscriberTypes.PERSONAL : SubscriberTypes.GROUP,
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

export const getSubscriptionChannel = (isPersonal: boolean, subscription?: DataHubSubscription) => {
    const subUserHandle = subscription?.notificationConfig?.notificationSettings?.slackSettings.userHandle || undefined;
    const subChannels = subscription?.notificationConfig?.notificationSettings?.slackSettings?.channels;
    const subGroupChannel = subChannels?.length ? subChannels[0] : undefined;
    return isPersonal ? subUserHandle : subGroupChannel;
};

export const getSettingsChannel = (
    isPersonal: boolean,
    userNotificationSettings?: GetUserNotificationSettingsQuery,
    groupNotificationSettings?: GetGroupNotificationSettingsQuery,
) => {
    const settingsUserHandle =
        userNotificationSettings?.getUserNotificationSettings?.slackSettings?.userHandle || undefined;
    const settingsChannels = groupNotificationSettings?.getGroupNotificationSettings?.slackSettings?.channels;
    const settingsGroupChannel = settingsChannels?.length ? settingsChannels[0] : undefined;
    return isPersonal ? settingsUserHandle : settingsGroupChannel;
};
