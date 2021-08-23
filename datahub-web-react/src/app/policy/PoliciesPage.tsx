import React, { useState } from 'react';
import { Button, List, Row, Space, Typography } from 'antd';
import { SearchablePage } from '../search/SearchablePage';
import AddPolicyModal from './AddPolicyModal';
import { COMMON_PRIVILEGES } from './entityPrivileges';
import { EntityType } from '../../types.generated';

export const PoliciesPage = () => {
    const [showAddPolicyModal, setShowAddPolicyModal] = useState(false);

    // const { data: policyData, loading: policyLoading, error: policyError } = useGetPoliciesQuery();

    const policies = [
        {
            urn: 'urn:li:policy:123243', // Auto generate a primary key for a policy.
            name: 'My test Policy',
            description: 'This policy is used for x y and z',
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
    ];

    const onClickNewPolicy = () => {
        setShowAddPolicyModal(true);
    };

    const onCancelNewPolicy = () => {
        setShowAddPolicyModal(false);
    };

    const policyPreview = (policy: any) => {
        console.log(policy);
        return (
            <Row>
                <Space direction="vertical">
                    <Typography.Title level={4}>{policy.name}</Typography.Title>
                    <Typography.Text type="secondary">{policy.description}</Typography.Text>

                    <Typography.Title level={5}>State</Typography.Title>
                    <Typography.Text>{policy.state}</Typography.Text>

                    <Typography.Title level={5}>Asset</Typography.Title>
                    <Typography.Text>{policy.resource.type}</Typography.Text>
                    <Typography.Text>{policy.resource.urns}</Typography.Text>

                    <Typography.Title level={5}>Privileges</Typography.Title>
                    <Typography.Text>{policy.privileges}</Typography.Text>

                    <Typography.Title level={5}>Actors</Typography.Title>
                    <Typography.Text>{policy.actors.appliesToOwners}</Typography.Text>
                    <Typography.Text>{policy.actors.users}</Typography.Text>
                    <Typography.Text>{policy.actors.groups}</Typography.Text>
                </Space>
            </Row>
        );
    };

    return (
        <SearchablePage>
            <Typography.Title level={3}>Your Active Policies</Typography.Title>
            <Button onClick={onClickNewPolicy} data-testid="add-policy-button">
                + New Policy
            </Button>
            <List
                bordered
                dataSource={policies}
                renderItem={(policy) => <List.Item>{policyPreview(policy)}</List.Item>}
            />
            <AddPolicyModal visible={showAddPolicyModal} onClose={onCancelNewPolicy} />
        </SearchablePage>
    );
};
