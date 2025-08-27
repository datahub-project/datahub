import { Button, Divider, Empty, Modal, Tag, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import AvatarsGroup from '@app/permissions/AvatarsGroup';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, DataHubPolicy, DataHubRole } from '@types';

/**
 * Converts technical permission names to user-friendly names
 */
const getPermissionFriendlyName = (permission: string): string => {
    const permissionMap: Record<string, string> = {
        // Platform permissions
        MANAGE_POLICIES: 'Manage Policies',
        MANAGE_USERS_AND_GROUPS: 'Manage Users & Groups',
        MANAGE_ROLES: 'Manage Roles',
        VIEW_ANALYTICS: 'View Analytics',
        GENERATE_PERSONAL_ACCESS_TOKENS: 'Generate Personal Access Tokens',
        MANAGE_DOMAINS: 'Manage Domains',
        MANAGE_GLOBAL_ANNOUNCEMENTS: 'Manage Global Announcements',
        MANAGE_TESTS: 'Manage Tests',
        MANAGE_GLOSSARIES: 'Manage Glossaries',
        MANAGE_TAGS: 'Manage Tags',
        MANAGE_BUSINESS_ATTRIBUTE: 'Manage Business Attributes',
        MANAGE_DOCUMENTATION: 'Manage Documentation',
        MANAGE_LINEAGE: 'Manage Lineage',
        MANAGE_OWNERSHIP: 'Manage Ownership',
        MANAGE_ASSERTIONS: 'Manage Assertions',
        MANAGE_INCIDENTS: 'Manage Incidents',
        MANAGE_SECRETS: 'Manage Secrets',
        MANAGE_INGESTION: 'Manage Ingestion',
        MANAGE_MONITORS: 'Manage Monitors',
        CREATE_DOMAINS: 'Create Domains',
        CREATE_TAGS: 'Create Tags',
        MANAGE_STRUCTURED_PROPERTIES: 'Manage Structured Properties',
        MANAGE_FORMS: 'Manage Forms',
        MANAGE_CONNECTIONS: 'Manage Connections',

        // Metadata permissions
        EDIT_ENTITY_TAGS: 'Edit Tags',
        EDIT_ENTITY_GLOSSARY_TERMS: 'Edit Glossary Terms',
        EDIT_ENTITY_OWNERS: 'Edit Owners',
        EDIT_ENTITY_DOCS: 'Edit Documentation',
        EDIT_ENTITY_DOC_LINKS: 'Edit Documentation Links',
        EDIT_ENTITY_STATUS: 'Edit Status',
        EDIT_ENTITY_ASSERTIONS: 'Edit Assertions',
        EDIT_DATASET_COL_TAGS: 'Edit Dataset Column Tags',
        EDIT_DATASET_COL_GLOSSARY_TERMS: 'Edit Dataset Column Glossary Terms',
        EDIT_DATASET_COL_DESCRIPTION: 'Edit Dataset Column Description',
        EDIT_LINEAGE: 'Edit Lineage',
        EDIT_ENTITY_DEPRECATION: 'Edit Deprecation',
        EDIT_ENTITY_SOFT_DELETE: 'Edit Soft Delete',
        EDIT_ENTITY_DATA_PRODUCTS: 'Edit Data Products',
        EDIT_ENTITY_QUERIES: 'Edit Queries',
        EDIT_ENTITY_EMBED: 'Edit Embed',
        EDIT_ENTITY: 'Edit Entity',
        DELETE_ENTITY: 'Delete Entity',
        SEARCH_PRIVILEGE: 'Search',
        GET_ENTITY_PRIVILEGE: 'View Entity',
        GET_TIMELINE_PRIVILEGE: 'View Timeline',
        VIEW_ENTITY_PAGE: 'View Entity Page',
        EDIT_CONTACT_INFO: 'Edit Contact Info',
        EDIT_USER_PROFILE: 'Edit User Profile',
        EDIT_GROUP_MEMBERS: 'Edit Group Members',
    };

    return permissionMap[permission] || permission.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase());
};

type Props = {
    role: DataHubRole;
    open: boolean;
    onClose: () => void;
};

const PolicyContainer = styled.div`
    padding-left: 20px;
    padding-right: 20px;
    > div {
        margin-bottom: 32px;
    }
`;

const ButtonsContainer = styled.div`
    display: flex;
    width: 100%;
    justify-content: flex-end;
    align-items: center;
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

const PolicyItem = styled.div`
    margin-bottom: 8px;
    border: 1px solid #f0f0f0;
    border-radius: 6px;
    background-color: #fafafa;
`;

const PolicyHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 12px;
    cursor: pointer;

    &:hover {
        background-color: #f5f5f5;
    }
`;

const PolicyInfo = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const PolicyName = styled(Typography.Text)`
    font-weight: 500;
    margin-bottom: 2px;
`;

const PolicyDescription = styled(Typography.Text)`
    font-size: 12px;
    color: #666;
`;

const PolicyDetails = styled.div`
    padding: 12px;
    border-top: 1px solid #f0f0f0;
    background-color: #fff;
`;

const DetailRow = styled.div`
    display: flex;
    margin-bottom: 8px;

    &:last-child {
        margin-bottom: 0;
    }
`;

const DetailLabel = styled(Typography.Text)`
    font-weight: 500;
    width: 100px;
    flex-shrink: 0;
`;

const DetailValue = styled(Typography.Text)`
    flex: 1;
`;

/**
 * Component used for displaying the details about an existing Role.
 */
export default function RoleDetailsModal({ role, open, onClose }: Props) {
    const entityRegistry = useEntityRegistry();
    const [expandedPolicies, setExpandedPolicies] = useState<Set<string>>(new Set());

    const togglePolicyExpansion = (policyUrn: string) => {
        const newExpanded = new Set(expandedPolicies);
        if (newExpanded.has(policyUrn)) {
            newExpanded.delete(policyUrn);
        } else {
            newExpanded.add(policyUrn);
        }
        setExpandedPolicies(newExpanded);
    };

    const actionButtons = (
        <ButtonsContainer>
            <Button onClick={onClose}>Close</Button>
        </ButtonsContainer>
    );

    const castedRole = role as any;

    const users = castedRole?.users?.relationships?.map((relationship) => relationship.entity as CorpUser);
    const policies = castedRole?.policies?.relationships?.map((relationship) => relationship.entity as DataHubPolicy);

    return (
        <Modal title={role?.name} open={open} onCancel={onClose} closable width={800} footer={actionButtons}>
            <PolicyContainer>
                <div>
                    <Typography.Title level={5}>Description</Typography.Title>
                    <ThinDivider />
                    <Typography.Text type="secondary">{role?.description}</Typography.Text>
                </div>
                <div>
                    <Typography.Title level={5}>Users</Typography.Title>
                    <ThinDivider />
                    <AvatarsGroup users={users} entityRegistry={entityRegistry} maxCount={50} size={28} />
                </div>
                <div>
                    <Typography.Title level={5}>Associated Policies ({policies?.length || 0})</Typography.Title>
                    <ThinDivider />
                    {policies && policies.length > 0 ? (
                        policies.map((policy) => {
                            const isExpanded = expandedPolicies.has(policy.urn);
                            // Enhanced type checking - check both type field and policy name
                            const isPlatformPolicy =
                                (policy.type as string)?.toUpperCase() === 'PLATFORM' ||
                                policy.name?.toLowerCase().includes('platform');
                            const policyType = isPlatformPolicy ? 'Platform' : 'Metadata';

                            return (
                                <PolicyItem key={policy.urn}>
                                    <PolicyHeader onClick={() => togglePolicyExpansion(policy.urn)}>
                                        <PolicyInfo>
                                            <PolicyName>{policy.name}</PolicyName>
                                            {policy.description && (
                                                <PolicyDescription>{policy.description}</PolicyDescription>
                                            )}
                                        </PolicyInfo>
                                        <Tag color={isPlatformPolicy ? 'blue' : 'green'}>{policyType}</Tag>
                                    </PolicyHeader>
                                    {isExpanded && (
                                        <PolicyDetails>
                                            <DetailRow>
                                                <DetailLabel>Type:</DetailLabel>
                                                <DetailValue>{policyType}</DetailValue>
                                            </DetailRow>
                                            {policy.description && (
                                                <DetailRow>
                                                    <DetailLabel>Description:</DetailLabel>
                                                    <DetailValue>{policy.description}</DetailValue>
                                                </DetailRow>
                                            )}
                                            <DetailRow>
                                                <DetailLabel>Permissions:</DetailLabel>
                                                <DetailValue>
                                                    {(policy as any).privileges
                                                        ?.map((p: string) => getPermissionFriendlyName(p))
                                                        .join(', ') || 'Not specified'}
                                                </DetailValue>
                                            </DetailRow>
                                            <DetailRow>
                                                <DetailLabel>Resource Filter:</DetailLabel>
                                                <DetailValue>
                                                    {(policy as any).resources?.filter
                                                        ? `${
                                                              (policy as any).resources.filter.allResources
                                                                  ? 'All Resources'
                                                                  : (policy as any).resources.filter.resources?.join(
                                                                        ', ',
                                                                    ) || 'Specific resources'
                                                          }`
                                                        : 'All Resources'}
                                                </DetailValue>
                                            </DetailRow>
                                            <DetailRow>
                                                <DetailLabel>State:</DetailLabel>
                                                <DetailValue>
                                                    <Tag color={(policy as any).state === 'ACTIVE' ? 'green' : 'red'}>
                                                        {(policy as any).state || 'ACTIVE'}
                                                    </Tag>
                                                </DetailValue>
                                            </DetailRow>
                                        </PolicyDetails>
                                    )}
                                </PolicyItem>
                            );
                        })
                    ) : (
                        <Empty
                            image={Empty.PRESENTED_IMAGE_SIMPLE}
                            description="No policies associated with this role"
                            style={{ margin: '16px 0' }}
                        />
                    )}
                </div>
            </PolicyContainer>
        </Modal>
    );
}
