import React from 'react';
import { Typography } from 'antd';
import { DataNode } from 'antd/lib/tree';
import styled from 'styled-components/macro';
import { EntityType } from '../../../../types.generated';

const NotificationTypeText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    margin-right: 8px;
`;

const ASSERTION_NODE_KEY = 'failing_assertions';
const INCIDENTS_NODE_KEY = 'raised_incidents';
const DEPRECATION_NODE_KEY = 'deprecation';
const DOCUMENTATION_NODE_KEY = 'documentation_changes';
const SCHEMA_NODE_KEY = 'schema_changes';
const OPERATIONAL_METADATA_NODE_KEY = 'operational_metadata';
const DATA_GOVERNANCE_NODE_KEY = 'data_governance';
const DATA_PROFILE_NODE_KEY = 'data_profile';

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
            <NotificationTypeText>An entity has been deprecated</NotificationTypeText>
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
            key: 'schema_changes_column_added',
            title: (
                <>
                    <NotificationTypeText>A column is added</NotificationTypeText>
                </>
            ),
        },
        {
            key: 'schema_changes_column_removed',
            title: (
                <>
                    <NotificationTypeText>A column is removed</NotificationTypeText>
                </>
            ),
        },
        {
            key: 'schema_changes_column_type_changed',
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
    title: <NotificationTypeText>Operational metadata events</NotificationTypeText>,
    children: [
        {
            key: 'operational_metadata_rows_inserted',
            title: (
                <>
                    <NotificationTypeText>Rows are inserted</NotificationTypeText>
                </>
            ),
        },
        {
            key: 'operational_metadata_rows_updated',
            title: (
                <>
                    <NotificationTypeText>Rows are updated</NotificationTypeText>
                </>
            ),
        },
        {
            key: 'operational_metadata_dataset_dropped',
            title: (
                <>
                    <NotificationTypeText>A dataset is dropped</NotificationTypeText>
                </>
            ),
        },
        {
            key: 'operational_metadata_dataset_altered',
            title: (
                <>
                    <NotificationTypeText>A dataset is altered</NotificationTypeText>
                </>
            ),
        },
    ],
};

const dataGovernanceNodeChildren: DataNode[] = [
    {
        key: 'data_governance_ownership_changes',
        title: (
            <>
                <NotificationTypeText>Ownership changes</NotificationTypeText>
            </>
        ),
    },
    {
        key: 'data_governance_glossary_terms_change',
        title: (
            <>
                <NotificationTypeText>Glossary terms are proposed, added, or removed</NotificationTypeText>
            </>
        ),
    },
    {
        key: 'data_governance_tags_change',
        title: (
            <>
                <NotificationTypeText>Tags are proposed, added or removed</NotificationTypeText>
            </>
        ),
    },
];

const dataProfileNode: DataNode = {
    key: DATA_PROFILE_NODE_KEY,
    title: <NotificationTypeText>A data profile changes </NotificationTypeText>,
};

const getDataGovernanceNode = (entityType: string): DataNode => {
    return {
        key: DATA_GOVERNANCE_NODE_KEY,
        title: <NotificationTypeText>Data governance events</NotificationTypeText>,
        children: [...dataGovernanceNodeChildren, ...(entityType === EntityType.Dataset ? [dataProfileNode] : [])],
    };
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
                getDataGovernanceNode(entityType),
            ];
            break;
        default:
            return [
                assertionsNode,
                incidentsNode,
                deprecationNode,
                documentationNode,
                getDataGovernanceNode(entityType),
            ];
            break;
    }
};
