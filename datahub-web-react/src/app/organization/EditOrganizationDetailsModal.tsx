import React, { useState } from 'react';
import { Modal, Form, Input, message } from 'antd';
import styled from 'styled-components';
import { useUpdateOrganizationMutation } from '@graphql/organization.generated';
import { useRefetch } from '@app/entity/shared/EntityContext';

const StyledFormItem = styled(Form.Item)`
    margin-bottom: 16px;
`;

const { TextArea } = Input;

interface Props {
    urn: string;
    name: string;
    description?: string | null;
    visible: boolean;
    onClose: () => void;
}

export const EditOrganizationDetailsModal = ({ urn, name, description, visible, onClose }: Props) => {
    const [form] = Form.useForm();
    const refetch = useRefetch();
    const [updateOrganization, { loading }] = useUpdateOrganizationMutation();

    const handleSubmit = async () => {
        try {
            const values = await form.validateFields();

            await updateOrganization({
                variables: {
                    urn,
                    input: {
                        name: values.name,
                        description: values.description || null,
                    },
                },
            });

            message.success('Organization updated successfully');
            refetch?.();
            onClose();
        } catch (e: any) {
            message.error(`Failed to update organization: ${e.message || 'Unknown error'}`);
        }
    };

    return (
        <Modal
            title="Edit Organization Details"
            open={visible}
            onCancel={onClose}
            onOk={handleSubmit}
            confirmLoading={loading}
            okText="Save"
            cancelText="Cancel"
            width={600}
        >
            <Form
                form={form}
                layout="vertical"
                initialValues={{
                    name,
                    description: description || '',
                }}
            >
                <StyledFormItem
                    name="name"
                    label="Name"
                    rules={[
                        { required: true, message: 'Please enter an organization name' },
                        { min: 1, message: 'Name must be at least 1 character' },
                        { max: 100, message: 'Name cannot exceed 100 characters' },
                    ]}
                >
                    <Input placeholder="Enter organization name" />
                </StyledFormItem>

                <StyledFormItem
                    name="description"
                    label="Description"
                    rules={[
                        { max: 500, message: 'Description cannot exceed 500 characters' },
                    ]}
                >
                    <TextArea
                        rows={4}
                        placeholder="Enter a description for this organization (optional)"
                    />
                </StyledFormItem>
            </Form>
        </Modal>
    );
};
