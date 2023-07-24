import React, { Key } from 'react';
import { Typography, message, notification } from 'antd';
import { DataNode } from 'antd/lib/tree';
import { CheckCircleFilled } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { EntityChangeType, EntityType } from '../../../../types.generated';

const NotificationTypeText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    margin-right: 8px;
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
            break;
        default:
            return [
                DEPRECATION_NODE_KEY,
                OWNERSHIP_CHANGE_NODE_KEY,
                GLOSSARY_TERM_CHANGE_NODE_KEY,
                TAG_CHANGE_NODE_KEY,
            ];
            break;
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
            break;
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
            break;
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
            title: <NotificationTypeText>Raised incidents</NotificationTypeText>,
        },
        {
            key: EntityChangeType.IncidentResolved,
            title: <NotificationTypeText>Resolved incidents</NotificationTypeText>,
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
            title: <NotificationTypeText>A column is modified</NotificationTypeText>,
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
            title: <NotificationTypeText>A new glossary term is proposed</NotificationTypeText>,
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
            title: <NotificationTypeText>A new tag is proposed</NotificationTypeText>,
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
            break;
        default:
            return [
                assertionsNode,
                incidentsNode,
                deprecationNode,
                ownershipChangeNode,
                glossaryTermChangeNode,
                tagChangeNode,
            ];
            break;
    }
};

export const deleteSubscriptionFunction = (subscriptionUrn: string, deleteSubscription, refetch) => {
    deleteSubscription({
        variables: {
            input: { subscriptionUrn },
        },
    })
        .then(() => {
            refetch?.();
            notification.success({
                message: `Success`,
                description: 'You have unsubscribed from this entity.',
                placement: 'bottomLeft',
                duration: 3,
            });
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

export const createSubscriptionFunction = (
    createSubscription,
    refetch,
    groupUrn,
    entityUrn,
    subscriptionTypes,
    entityChangeTypes,
    sinkTypes,
    notificationSettings,
) => {
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
            setTimeout(() => {
                refetch();
            }, 3000);
        })
        .catch((e: unknown) => {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to create subscription`, duration: 3 });
            }
        });
};

export const updateSubscriptionFunction = (
    updateSubscription,
    refetch,
    subscription,
    subscriptionTypes,
    entityChangeTypes,
    sinkTypes,
    notificationSettings,
) => {
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
                setTimeout(() => {
                    refetch();
                }, 3000);
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
