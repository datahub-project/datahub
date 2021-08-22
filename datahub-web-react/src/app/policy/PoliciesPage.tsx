import React, { useState } from 'react';
import { Button, List, Typography } from 'antd';
import { SearchablePage } from '../search/SearchablePage';
import AddPolicyModal from './AddPolicyModal';

export const PoliciesPage = () => {
    const [showAddPolicyModal, setShowAddPolicyModal] = useState(false);

    // const { data: policyData, loading: policyLoading, error: policyError } = useGetPoliciesQuery();

    const policies = [];

    const onClickNewPolicy = () => {
        setShowAddPolicyModal(true);
    };

    const onCancelNewPolicy = () => {
        setShowAddPolicyModal(false);
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
                renderItem={(_) => (
                    <List.Item>
                        <div>Heres the policy data.</div>
                    </List.Item>
                )}
            />
            <AddPolicyModal visible={showAddPolicyModal} onClose={onCancelNewPolicy} />
        </SearchablePage>
    );
};
