import React from 'react';
import { Form, Input, Select, Typography } from 'antd';

type Props = {
    policyType: string;
    setPolicyType: (type: string) => void;
    policyName: string;
    setPolicyName: (name: string) => void;
    policyDescription: string;
    setPolicyDescription: (description: string) => void;
    updateStepCompletion: (isComplete: boolean) => void;
};

// TODO: Fix initial state problem.
export default function PolicyTypeForm({
    policyType,
    setPolicyType,
    policyName,
    setPolicyName,
    policyDescription,
    setPolicyDescription,
    updateStepCompletion,
}: Props) {
    const updatePolicyName = (name: string) => {
        setPolicyName(name);
        if (name.length > 0) {
            updateStepCompletion(true);
        } else {
            updateStepCompletion(false);
        }
    };

    return (
        <Form layout="vertical" style={{ margin: 12, marginTop: 36 }}>
            <Form.Item name="policyName" labelAlign="right" label={<Typography.Text strong>Name</Typography.Text>}>
                <Typography.Paragraph>A name for your new policy.</Typography.Paragraph>
                <Input
                    placeholder="Your policy name"
                    value={policyName}
                    onChange={(event) => updatePolicyName(event.target.value)}
                />
            </Form.Item>
            <Form.Item name="policyType" label={<Typography.Text strong>Type</Typography.Text>}>
                <Typography.Paragraph>The type of policy you would like to create.</Typography.Paragraph>

                <Select defaultValue={policyType} onSelect={setPolicyType}>
                    <Select.Option value="Platform">Platform</Select.Option>
                    <Select.Option value="Metadata">Metadata</Select.Option>
                </Select>
            </Form.Item>
            <Typography.Paragraph type="secondary">
                The <b>Platform</b> policy type allows you to assign top-level DataHub Platform privileges to users.
                These include managing users and groups, creating policies, viewing analytics dashboards and more.
                <br />
                <br />
                The <b>Metadata</b> policy type allows you to assign metadata privileges to users. These include the
                ability to manipulate metadata like ownership, tags, documentation associated with Datasets, Charts,
                Dashboards, & more.
            </Typography.Paragraph>
            <Form.Item
                name="policyDescription"
                labelAlign="right"
                label={<Typography.Text strong>Description</Typography.Text>}
            >
                <Typography.Paragraph>An optional description for your new policy.</Typography.Paragraph>
                <Input
                    placeholder="Your policy description"
                    value={policyDescription}
                    onChange={(event) => setPolicyDescription(event.target.value)}
                />
            </Form.Item>
        </Form>
    );
}
