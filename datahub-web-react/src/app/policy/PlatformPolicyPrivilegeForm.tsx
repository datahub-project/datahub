import React from 'react';
import { Form, Select, Tag, Typography } from 'antd';
import { PLATFORM_PRIVILEGES } from './privileges';

type Props = {
    privileges: Array<string>;
    setPrivileges: (newPrivs: Array<string>) => void;
    updateStepCompletion: (isComplete: boolean) => void;
};

// TODO: Consider merging this with MetadataPolicyPrivilegeForm.
export default function PlatformPolicyPrivilegeForm({ privileges, setPrivileges, updateStepCompletion }: Props) {
    const privilegeOptions = PLATFORM_PRIVILEGES;

    const onSelectPrivilege = (privilege: string) => {
        updateStepCompletion(true);
        const newPrivs = [...privileges, privilege];
        setPrivileges(newPrivs as never[]);
    };

    const onDeselectPrivilege = (privilege: string) => {
        const newPrivs = privileges.filter((priv) => priv !== privilege);
        setPrivileges(newPrivs as never[]);
        if (newPrivs.length === 0) {
            updateStepCompletion(false);
        }
    };

    return (
        <Form layout="vertical" initialValues={{}} style={{ margin: 12, marginTop: 36, marginBottom: 40 }}>
            <>
                <Form.Item
                    label={<Typography.Text strong>Privileges</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: 'Please select your permissions.',
                            type: 'array',
                        },
                    ]}
                >
                    <Typography.Paragraph>Select a set of privileges to permit.</Typography.Paragraph>
                    <Select
                        defaultValue={privileges}
                        mode="multiple"
                        onSelect={(value: string) => onSelectPrivilege(value)}
                        onDeselect={(value: any) => onDeselectPrivilege(value)}
                        tagRender={(tagProps) => (
                            <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                {tagProps.label}
                            </Tag>
                        )}
                    >
                        {privilegeOptions.map((priv) => (
                            <Select.Option value={priv.type}>{priv.displayName}</Select.Option>
                        ))}
                        <Select.Option value="all">All Privileges</Select.Option>
                    </Select>
                </Form.Item>
            </>
        </Form>
    );
}
