import React, { Key } from 'react';
import { Tooltip, Typography, message, notification } from 'antd';
import { DataNode } from 'antd/lib/tree';
import { CheckCircleFilled, QuestionCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import {
    DataHubSubscription,
    EntityChangeType,
    EntityType,
    NotificationSettingsInput,
    NotificationSinkType,
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
const SCHEMA_NODE_CHILDREN = [
    EntityChangeType.OperationColumnAdded,
    EntityChangeType.OperationColumnRemoved,
    EntityChangeType.OperationColumnModified,
];
const OWNERSHIP_CHANGE_NODE_KEY = 'ownership_change';
const OWNERSHIP_CHANGE_NODE_CHILDREN = [EntityChangeType.OwnerAdded, EntityChangeType.OwnerRemoved];
const GLOSSARY_TERM_CHANGE_NODE_KEY = 'glossary_term_change';
const GLOSSARY_TERM_CHANGE_NODE_CHILDREN = [
    EntityChangeType.GlossaryTermAdded,
    EntityChangeType.GlossaryTermRemoved,
    EntityChangeType.GlossaryTermProposed,
];
const TAG_CHANGE_NODE_KEY = 'tag_change';
const TAG_CHANGE_NODE_CHILDREN = [EntityChangeType.TagAdded, EntityChangeType.TagRemoved, EntityChangeType.TagProposed];

const NESTED_NODE_KEY_PARENTS = new Set([
    SCHEMA_NODE_KEY,
    OWNERSHIP_CHANGE_NODE_KEY,
    GLOSSARY_TERM_CHANGE_NODE_KEY,
    TAG_CHANGE_NODE_KEY,
    ASSERTION_NODE_KEY,
    INCIDENTS_NODE_KEY,
]);

export const getDefaultSelectedKeys = (entityType: EntityType): string[] => {
    switch (entityType) {
        case EntityType.Dataset:
            return [
                ASSERTION_NODE_KEY,
                INCIDENTS_NODE_KEY,
                DEPRECATION_NODE_KEY,
                SCHEMA_NODE_KEY,
                OWNERSHIP_CHANGE_NODE_KEY,
                GLOSSARY_TERM_CHANGE_NODE_KEY,
                TAG_CHANGE_NODE_KEY,
            ];
        default:
            return [
                DEPRECATION_NODE_KEY,
                OWNERSHIP_CHANGE_NODE_KEY,
                GLOSSARY_TERM_CHANGE_NODE_KEY,
                TAG_CHANGE_NODE_KEY,
            ];
    }
};

export const getDefaultCheckedKeys = (entityType: EntityType): string[] => {
    switch (entityType) {
        case EntityType.Dataset:
            return [
                ASSERTION_NODE_KEY,
                INCIDENTS_NODE_KEY,
                DEPRECATION_NODE_KEY,
                SCHEMA_NODE_KEY,
                ...SCHEMA_NODE_CHILDREN,
                OWNERSHIP_CHANGE_NODE_KEY,
                ...OWNERSHIP_CHANGE_NODE_CHILDREN,
                GLOSSARY_TERM_CHANGE_NODE_KEY,
                ...GLOSSARY_TERM_CHANGE_NODE_CHILDREN,
                TAG_CHANGE_NODE_KEY,
                ...TAG_CHANGE_NODE_CHILDREN,
            ];
        default:
            return [
                DEPRECATION_NODE_KEY,
                OWNERSHIP_CHANGE_NODE_KEY,
                ...OWNERSHIP_CHANGE_NODE_CHILDREN,
                GLOSSARY_TERM_CHANGE_NODE_KEY,
                ...GLOSSARY_TERM_CHANGE_NODE_CHILDREN,
                TAG_CHANGE_NODE_KEY,
                ...TAG_CHANGE_NODE_CHILDREN,
            ];
    }
};

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
    subscriptionUrn,
    deleteSubscription,
    onRefetch,
}: {
    subscriptionUrn: string;
    deleteSubscription: ReturnType<typeof useDeleteSubscriptionMutation>[0];
    onRefetch?: () => void;
}) => {
    deleteSubscription({
        variables: {
            input: { subscriptionUrn },
        },
    })
        .then(() => {
            notification.success({
                message: `Success`,
                description: 'You have unsubscribed from this entity.',
                placement: 'bottomLeft',
                duration: 3,
            });
            if (onRefetch) window.setTimeout(onRefetch, REFETCH_DELAY);
        })
        .catch((e: unknown) => {
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
    groupUrn,
    entityUrn,
    subscriptionTypes,
    entityChangeTypes,
    sinkTypes,
    notificationSettings,
    onRefetch,
}: {
    createSubscription: ReturnType<typeof useCreateSubscriptionMutation>[0];
    groupUrn: string | undefined;
    entityUrn: string;
    subscriptionTypes: Array<SubscriptionType>;
    entityChangeTypes: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    notificationSettings: NotificationSettingsInput | undefined;
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
                    sinkTypes,
                    notificationSettings,
                },
            },
        },
    })
        .then(() => {
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
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to create subscription`, duration: 3 });
            }
        });
};

export const updateSubscriptionFunction = ({
    updateSubscription,
    subscription,
    subscriptionTypes,
    entityChangeTypes,
    sinkTypes,
    notificationSettings,
    onRefetch,
}: {
    updateSubscription: ReturnType<typeof useUpdateSubscriptionMutation>[0];
    subscription: DataHubSubscription | undefined;
    subscriptionTypes: Array<SubscriptionType>;
    entityChangeTypes: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    notificationSettings: NotificationSettingsInput | undefined;
    onRefetch?: () => void;
}) => {
    if (subscription && subscription.subscriptionUrn) {
        updateSubscription({
            variables: {
                input: {
                    subscriptionUrn: subscription?.subscriptionUrn,
                    subscriptionTypes,
                    entityChangeTypes,
                    notificationConfig: {
                        sinkTypes,
                        notificationSettings,
                    },
                },
            },
        })
            .then(() => {
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

export const getUserSettingsChannel = (
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
