import { Form, Input, Modal, message } from 'antd';
import React from 'react';

import { useCreateOrganizationMutation } from '@graphql/organization.generated';

interface Props {
    visible: boolean;
    onClose: () => void;
    onCreate: () => void;
}

export const CreateOrganizationModal = ({ visible, onClose, onCreate }: Props) => {
    const [form] = Form.useForm();
    const [createOrganization, { loading }] = useCreateOrganizationMutation();

    const handleCreate = async () => {
        try {
            const values = await form.validateFields();
            // Generate a simple ID from the name (slugify)
            const id = values.name.toLowerCase().replace(/[^a-z0-9]/g, '-');

            await createOrganization({
                variables: {
                    input: {
                        id,
                        name: values.name,
                        description: values.description,
                    },
                },
            });
            message.success('Organization created successfully');
            form.resetFields();
            onCreate();
        } catch (e: any) {
            message.error(e.message || 'Failed to create organization');
        }
    };

    return (
        <Modal
            title="Create Organization"
            open={visible}
            onCancel={onClose}
            onOk={handleCreate}
            confirmLoading={loading}
        >
            <Form form={form} layout="vertical">
                <Form.Item
                    name="name"
                    label="Name"
                    rules={[{ required: true, message: 'Please enter organization name' }]}
                >
                    <Input />
                </Form.Item>
                <Form.Item name="description" label="Description">
                    <Input.TextArea />
                </Form.Item>
            </Form>
        </Modal>
    );
};
