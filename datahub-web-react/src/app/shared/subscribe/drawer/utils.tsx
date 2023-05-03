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

const ASSERTION_NODE_KEY = EntityChangeType.AssertionFailed;
const INCIDENTS_NODE_KEY = EntityChangeType.IncidentRaised;
const DEPRECATION_NODE_KEY = EntityChangeType.Deprecated;
const DOCUMENTATION_NODE_KEY = EntityChangeType.DocumentationChange;
const SCHEMA_NODE_KEY = 'schema_changes';
const SCHEMA_NODE_CHILDREN = [
    EntityChangeType.OperationColumnAdded,
    EntityChangeType.OperationColumnRemoved,
    EntityChangeType.OperationColumnModified,
];
const OPERATIONAL_METADATA_NODE_KEY = 'operational_metadata';
const OPERATIONAL_METADATA_NODE_CHILDREN = [
    EntityChangeType.OperationRowsInserted,
    EntityChangeType.OperationRowsRemoved,
    EntityChangeType.OperationRowsUpdated,
];
const DATA_GOVERNANCE_NODE_KEY = 'data_governance';
const DATA_GOVERNANCE_NODE_CHILDREN = [
    EntityChangeType.OwnershipChange,
    EntityChangeType.GlossaryTermChange,
    EntityChangeType.TagChange,
];

export const getDefaultSelectedKeys = (entityType: EntityType): string[] => {
    switch (entityType) {
        case EntityType.Dataset:
            return [
                ASSERTION_NODE_KEY,
                INCIDENTS_NODE_KEY,
                DEPRECATION_NODE_KEY,
                DOCUMENTATION_NODE_KEY,
                SCHEMA_NODE_KEY,
                OPERATIONAL_METADATA_NODE_KEY,
                DATA_GOVERNANCE_NODE_KEY,
            ];
            break;
        default:
            return [
                ASSERTION_NODE_KEY,
                INCIDENTS_NODE_KEY,
                DEPRECATION_NODE_KEY,
                DOCUMENTATION_NODE_KEY,
                DATA_GOVERNANCE_NODE_KEY,
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
                DOCUMENTATION_NODE_KEY,
                SCHEMA_NODE_KEY,
                ...SCHEMA_NODE_CHILDREN,
                OPERATIONAL_METADATA_NODE_KEY,
                ...OPERATIONAL_METADATA_NODE_CHILDREN,
                DATA_GOVERNANCE_NODE_KEY,
                ...DATA_GOVERNANCE_NODE_CHILDREN,
            ];
            break;
        default:
            return [
                ASSERTION_NODE_KEY,
                INCIDENTS_NODE_KEY,
                DEPRECATION_NODE_KEY,
                DOCUMENTATION_NODE_KEY,
                ...DATA_GOVERNANCE_NODE_CHILDREN,
            ];
            break;
    }
};

export const getEntityChangeTypesFromCheckedKeys = (checkedKeys: Key[]): EntityChangeType[] => {
    return checkedKeys.filter(
        (key) => key !== SCHEMA_NODE_KEY && key !== OPERATIONAL_METADATA_NODE_KEY && key !== DATA_GOVERNANCE_NODE_KEY,
    ) as EntityChangeType[];
};

const assertionsNode: DataNode = {
    key: ASSERTION_NODE_KEY,
    title: (
        <>
            <NotificationTypeText>Failing assertions</NotificationTypeText>
        </>
    ),
};

const incidentsNode: DataNode = {
    key: INCIDENTS_NODE_KEY,
    title: (
        <>
            <NotificationTypeText>Raised incidents</NotificationTypeText>
        </>
    ),
};

const deprecationNode: DataNode = {
    key: DEPRECATION_NODE_KEY,
    title: (
        <>
            <NotificationTypeText>Deprecation</NotificationTypeText>
        </>
    ),
};

const documentationNode: DataNode = {
    key: DOCUMENTATION_NODE_KEY,
    title: (
        <>
            <NotificationTypeText>Documentation changes</NotificationTypeText>
        </>
    ),
};

const schemaNode: DataNode = {
    key: SCHEMA_NODE_KEY,
    title: <NotificationTypeText>Schema changes</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.OperationColumnAdded,
            title: (
                <>
                    <NotificationTypeText>A column is added</NotificationTypeText>
                </>
            ),
        },
        {
            key: EntityChangeType.OperationColumnRemoved,
            title: (
                <>
                    <NotificationTypeText>A column is removed</NotificationTypeText>
                </>
            ),
        },
        {
            key: EntityChangeType.OperationColumnModified,
            title: (
                <>
                    <NotificationTypeText>A column type changed</NotificationTypeText>
                </>
            ),
        },
    ],
};

const operationalMetadataNode: DataNode = {
    key: OPERATIONAL_METADATA_NODE_KEY,
    title: <NotificationTypeText>Operational change events</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.OperationRowsInserted,
            title: (
                <>
                    <NotificationTypeText>Rows are inserted</NotificationTypeText>
                </>
            ),
        },
        {
            key: EntityChangeType.OperationRowsUpdated,
            title: (
                <>
                    <NotificationTypeText>Rows are updated</NotificationTypeText>
                </>
            ),
        },
        {
            key: EntityChangeType.OperationRowsRemoved,
            title: (
                <>
                    <NotificationTypeText>Rows are removed</NotificationTypeText>
                </>
            ),
        },
    ],
};

const dataGovernanceNode = {
    key: DATA_GOVERNANCE_NODE_KEY,
    title: <NotificationTypeText>Data governance events</NotificationTypeText>,
    children: [
        {
            key: EntityChangeType.OwnershipChange,
            title: (
                <>
                    <NotificationTypeText>Ownership are added or removed</NotificationTypeText>
                </>
            ),
        },
        {
            key: EntityChangeType.GlossaryTermChange,
            title: (
                <>
                    <NotificationTypeText>Glossary terms are proposed, added, or removed</NotificationTypeText>
                </>
            ),
        },
        {
            key: EntityChangeType.TagChange,
            title: (
                <>
                    <NotificationTypeText>Tags are proposed, added or removed</NotificationTypeText>
                </>
            ),
        },
    ],
};

export const getTreeDataForEntity = (entityType: string): DataNode[] => {
    switch (entityType) {
        case EntityType.Dataset:
            return [
                assertionsNode,
                incidentsNode,
                deprecationNode,
                documentationNode,
                schemaNode,
                operationalMetadataNode,
                dataGovernanceNode,
            ];
            break;
        default:
            return [assertionsNode, incidentsNode, deprecationNode, documentationNode, dataGovernanceNode];
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
