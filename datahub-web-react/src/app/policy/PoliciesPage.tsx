import React, { useState } from 'react';
import { Button, Col, Layout, List, Row, Typography } from 'antd';
import styled from 'styled-components';

import { SearchablePage } from '../search/SearchablePage';
import PolicyBuilderModal from './PolicyBuilderModal';
import { COMMON_PRIVILEGES } from './entityPrivileges';
import { EntityType } from '../../types.generated';
import PolicyListItem from './PolicyListItem';
import PolicyDetailsModal from './PolicyDetailsModal';

const PolicyList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 12px;
        padding: 16px 32px;
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
`;

// TODO: Cleanup the styling.
export const PoliciesPage = () => {
    const [showPolicyBuilderModal, setShowPolicyBuilderModal] = useState(false);
    const [showViewPolicyModal, setShowViewPolicyModal] = useState(false);
    const [focusPolicy, setFocusPolicy] = useState(undefined);

    // const { data: policyData, loading: policyLoading, error: policyError } = useGetPoliciesQuery();

    const policies = [
        {
            urn: 'urn:li:policy:123243', // Auto generate a primary key for a policy.
            name: 'Admin Dataset Edit Access',
            description: 'This policy grants edit access to all Datasets to the eng data stewards.',
            type: 'METADATA',
            state: 'ACTIVE',
            resource: {
                type: EntityType.Dataset,
                urns: ['urn:li:dataset:(urn:li:dataPlatform:(MY_SQL),mydataset,PROD)'],
            },
            privileges: COMMON_PRIVILEGES.map((priv) => priv.type),
            actors: {
                users: ['urn:li:corpuser:datahub'],
                groups: ['urn:li:corpGroup:bfoo'],
                appliesToOwners: true,
            },
            createdAt: 0,
            createdBy: 'urn:li:corpuser:jdoe',
        },
        {
            urn: 'urn:li:policy:123244', // Auto generate a primary key for a policy.
            name: 'Dashboard Owner Privileges Policy',
            description: 'This policy is used to grant specific edit privileges to dashboard owners.',
            type: 'METADATA',
            state: 'INACTIVE',
            resource: {
                type: EntityType.Dataset,
                urns: ['urn:li:dataset:(urn:li:dataPlatform:(MY_SQL),mydataset,PROD)'],
            },
            privileges: COMMON_PRIVILEGES.map((priv) => priv.type),
            actors: {
                users: ['urn:li:corpuser:datahub'],
                groups: ['urn:li:corpGroup:bfoo'],
                appliesToOwners: true,
            },
            createdAt: 0,
            createdBy: 'urn:li:corpuser:jdoe',
        },
    ];

    const onClickNewPolicy = () => {
        setFocusPolicy(undefined);
        setShowPolicyBuilderModal(true);
    };

    const onCancelPolicyBuilder = () => {
        setFocusPolicy(undefined);
        setShowPolicyBuilderModal(false);
    };

    const onViewPolicy = (policy: any) => {
        setShowViewPolicyModal(true);
        setFocusPolicy(policy);
    };

    const onCancelViewPolicy = () => {
        setShowViewPolicyModal(false);
        setFocusPolicy(undefined);
    };

    const onRemovePolicy = () => {
        console.log(`Removing a policy details ${focusPolicy}`);
    };

    const onEditPolicy = () => {
        setShowViewPolicyModal(false);
        setShowPolicyBuilderModal(true);
        console.log(`Editing a policy details ${focusPolicy}`);
    };

    const onToggleActive = () => {
        const active = (focusPolicy !== undefined && (focusPolicy as any).state === 'ACTIVE') || false;
        console.log(`Toggling a policy details ${focusPolicy} ${active}`);
    };

    return (
        <SearchablePage>
            <Layout style={{ padding: 40 }}>
                <Row justify="center">
                    <Col sm={24} md={24} lg={20} xl={20}>
                        <Typography.Title level={2} style={{ marginBottom: 24 }}>
                            Your Policies
                        </Typography.Title>
                        <Button onClick={onClickNewPolicy} style={{ marginBottom: 16 }} data-testid="add-policy-button">
                            + New Policy
                        </Button>
                        <PolicyList
                            bordered
                            dataSource={policies}
                            renderItem={(policy) => (
                                <PolicyListItem policy={policy} onView={() => onViewPolicy(policy)} />
                            )}
                        />
                        {showPolicyBuilderModal && (
                            <PolicyBuilderModal
                                policy={focusPolicy}
                                visible={showPolicyBuilderModal}
                                onClose={onCancelPolicyBuilder}
                            />
                        )}
                        {showViewPolicyModal && (
                            <PolicyDetailsModal
                                policy={focusPolicy}
                                visible={showViewPolicyModal}
                                onEdit={onEditPolicy}
                                onClose={onCancelViewPolicy}
                                onRemove={onRemovePolicy}
                                onToggleActive={onToggleActive}
                            />
                        )}
                    </Col>
                </Row>
            </Layout>
        </SearchablePage>
    );
};
