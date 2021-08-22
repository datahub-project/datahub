import React from 'react';
import { Form, Input, Select, Typography } from 'antd';

export default function PolicyActorForm() {
    return (
        <Form layout="horizontal" initialValues={{}}>
            <Typography.Title>Applies to</Typography.Title>
            <Form.Item label="Operator">
                <Select>
                    <Select.Option value="isResourceOwner">Is Owner</Select.Option>
                    <Select.Option value="isUser">Is User</Select.Option>
                    <Select.Option value="isMemberOfGroup">Is Member of Group</Select.Option>
                </Select>
            </Form.Item>
            <Form.Item label="User">
                <Input />
            </Form.Item>
        </Form>
    );
}
