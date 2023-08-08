import React from 'react';
import { Form, Input, Select, Typography } from 'antd';
import styled from 'styled-components';
import { PolicyType } from '../../../types.generated';

type Props = {
    policyType: string;
    setPolicyType: (type: PolicyType) => void;
    policyName: string;
    setPolicyName: (name: string) => void;
    policyDescription: string;
    setPolicyDescription: (description: string) => void;
};

const TypeForm = styled(Form)`
    margin: 12px;
    margin-top: 36px;
    > div {
        margin-bottom: 28px;
    }
`;

const TypeDescriptionParagraph = styled(Typography.Paragraph)`
    margin-top: 12px;
`;

export default function PolicyTypeForm({
    policyType,
    setPolicyType,
    policyName,
    setPolicyName,
    policyDescription,
    setPolicyDescription,
}: Props) {
    const updatePolicyName = (name: string) => {
        setPolicyName(name);
    };

    return (
        <TypeForm layout="vertical">
            <Form.Item name="policyName" labelAlign="right" label={<Typography.Text strong>Name</Typography.Text>}>
                <Typography.Paragraph>A name for your new policy.</Typography.Paragraph>
                <Input
                    placeholder="Your policy name"
                    data-testid="policy-name"
                    value={policyName}
                    onChange={(event) => updatePolicyName(event.target.value)}
                />
            </Form.Item>
            <Form.Item name="policyType" label={<Typography.Text strong>Type</Typography.Text>}>
                <Typography.Paragraph>The type of policy you would like to create.</Typography.Paragraph>
                <Select
                    data-testid="policy-type"
                    defaultValue={policyType}
                    onSelect={(value) => setPolicyType(value as PolicyType)}
                >
                    <Select.Option data-testid="platform" value={PolicyType.Platform}>
                        Platform
                    </Select.Option>
                    <Select.Option data-testid="metadata" value={PolicyType.Metadata}>
                        Metadata
                    </Select.Option>
                </Select>
                <TypeDescriptionParagraph type="secondary">
                    The <b>Platform</b> policy type allows you to assign top-level DataHub Platform privileges to users.
                    These include managing users and groups, creating policies, viewing analytics dashboards and more.
                    <br />
                    <br />
                    The <b>Metadata</b> policy type allows you to assign metadata privileges to users. These include the
                    ability to manipulate metadata like ownership, tags, documentation associated with Datasets, Charts,
                    Dashboards, & more.
                </TypeDescriptionParagraph>
            </Form.Item>
            <Form.Item
                name="policyDescription"
                labelAlign="right"
                label={<Typography.Text strong>Description</Typography.Text>}
            >
                <Typography.Paragraph>An optional description for your new policy.</Typography.Paragraph>
                <Input
                    placeholder="Your policy description"
                    data-testid="policy-description"
                    value={policyDescription}
                    onChange={(event) => setPolicyDescription(event.target.value)}
                />
            </Form.Item>
        </TypeForm>
    );
}
