import React from 'react';
import { Form, Select, Typography } from 'antd';

type Props = {
    selectPolicyType: (type: string) => void;
};

export default function PolicyTypeForm({ selectPolicyType }: Props) {
    return (
        <Form layout="horizontal" initialValues={{ item1: 'Metadata' }} style={{ margin: 12, marginTop: 36 }}>
            <Form.Item name="item1">
                <Select onSelect={selectPolicyType}>
                    <Select.Option value="Platform">Platform</Select.Option>
                    <Select.Option value="Metadata">Metadata</Select.Option>
                </Select>
            </Form.Item>
            <Typography.Paragraph>
                The <b>Platform</b> policy type allows you to assign top-level DataHub Platform privileges to users.
                These include managing users and groups, creating policies, viewing analytics dashboards and more.
                <br />
                <br />
                The <b>Metadata</b> policy type allows you to assign metadata privileges to users. These include the
                ability to manipulate metadata like ownership, tags, documentation associated with Datasets, Charts,
                Dashboards, & more.
            </Typography.Paragraph>
        </Form>
    );
}
