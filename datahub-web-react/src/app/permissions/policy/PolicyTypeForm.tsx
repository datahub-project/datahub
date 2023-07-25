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
            <Form.Item name="policyName" labelAlign="right" label={<Typography.Text strong>规则名称</Typography.Text>}>
                <Typography.Paragraph>您的规则名称.</Typography.Paragraph>
                <Input
                    placeholder="您的规则名称"
                    value={policyName}
                    onChange={(event) => updatePolicyName(event.target.value)}
                />
            </Form.Item>
            <Form.Item name="policyType" label={<Typography.Text strong>规则类型</Typography.Text>}>
                <Typography.Paragraph>您要创建的规则类型.</Typography.Paragraph>
                <Select defaultValue={policyType} onSelect={(value) => setPolicyType(value as PolicyType)}>
                    <Select.Option value={PolicyType.Platform}>Platform</Select.Option>
                    <Select.Option value={PolicyType.Metadata}>Metadata</Select.Option>
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
                label={<Typography.Text strong>规则说明</Typography.Text>}
            >
                <Typography.Paragraph>规则说明（可选）.</Typography.Paragraph>
                <Input
                    placeholder="规则说明"
                    value={policyDescription}
                    onChange={(event) => setPolicyDescription(event.target.value)}
                />
            </Form.Item>
        </TypeForm>
    );
}
