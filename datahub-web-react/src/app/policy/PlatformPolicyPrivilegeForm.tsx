import React from 'react';
import { Form, Select } from 'antd';

export default function PlatformPolicyPrivilegeForm() {
    return (
        <Form layout="horizontal" initialValues={{}}>
            <Form.Item label="PlatformPrivilege">
                <Select>
                    <Select.Option value="manageUsersGroups">Manage Users + Groups</Select.Option>
                    <Select.Option value="viewAnalytics">View Analytics</Select.Option>
                </Select>
            </Form.Item>
        </Form>
    );
}
