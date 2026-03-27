import { Form, Input, Select, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { PolicyEffect, PolicyType } from '@types';

type Props = {
    policyType: string;
    setPolicyType: (type: PolicyType) => void;
    policyEffect: PolicyEffect;
    setPolicyEffect: (effect: PolicyEffect) => void;
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
    policyEffect,
    setPolicyEffect,
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
            <Form.Item name="policyEffect" label={<Typography.Text strong>Effect</Typography.Text>}>
                <Typography.Paragraph>Whether this policy grants or denies access.</Typography.Paragraph>
                <Select
                    data-testid="policy-effect"
                    value={policyEffect}
                    onSelect={(value) => setPolicyEffect(value as PolicyEffect)}
                >
                    <Select.Option data-testid="allow" value={PolicyEffect.Allow}>
                        Allow
                    </Select.Option>
                    <Select.Option data-testid="deny" value={PolicyEffect.Deny}>
                        Deny
                    </Select.Option>
                </Select>
                <TypeDescriptionParagraph type="secondary">
                    <b>Allow</b> policies grant access to resources when matched. <b>Deny</b> policies explicitly deny
                    access and take precedence over Allow policies. Use Deny policies to restrict access to specific
                    users, groups, or resources even if they would normally be granted access by an Allow policy.
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
