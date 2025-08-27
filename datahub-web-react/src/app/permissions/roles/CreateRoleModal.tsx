import { Button, Divider, Form, Input, Modal, Select, Typography, message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useCreateRoleMutation } from '@graphql/mutations.generated';
import { useListPoliciesQuery } from '@graphql/policy.generated';

const { TextArea } = Input;
const { Title } = Typography;

/**
 * Modal for creating new custom roles in DataHub.
 * Allows users to define role name, optional description, and associate policies.
 */

const StyledModal = styled(Modal)`
    .ant-modal-content {
        border-radius: 8px;
    }
    .ant-modal-header {
        border-bottom: 1px solid #f0f0f0;
        padding: 16px 24px;
    }
    .ant-modal-body {
        padding: 24px;
    }
`;

const FormContainer = styled.div`
    padding: 16px 0;
`;

const StyledTitle = styled(Title)`
    margin-bottom: 24px !important;
`;

/**
 * Props for the CreateRoleModal component
 */
type Props = {
    /** Whether the modal is visible */
    visible: boolean;
    /** Callback when modal is closed */
    onClose: () => void;
    /** Optional callback when role is successfully created */
    onSuccess?: () => void;
};

/**
 * Form data structure for role creation
 */
interface CreateRoleFormData {
    /** Role name (required) */
    name: string;
    /** Role description (optional) */
    description?: string;
    /** Associated policy URNs (optional) */
    policyUrns?: string[];
}

/**
 * CreateRoleModal component for creating new custom roles.
 * Provides form validation, policy association, and consistent styling.
 */
export const CreateRoleModal = ({ visible, onClose, onSuccess }: Props) => {
    const [form] = Form.useForm<CreateRoleFormData>();
    const [createRole, { loading }] = useCreateRoleMutation();

    // Fetch available policies for association
    const { data: policiesData, loading: policiesLoading } = useListPoliciesQuery({
        variables: {
            input: {
                start: 0,
                count: 100, // Fetch first 100 policies for selection
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    /**
     * Handles role creation form submission with validation and error handling
     */
    const handleSubmit = async () => {
        try {
            const values = await form.validateFields();

            await createRole({
                variables: {
                    input: {
                        name: values.name.trim(),
                        description: values.description?.trim() || '', // Optional description
                        policyUrns: values.policyUrns || [],
                    },
                },
            });

            message.success('Role created successfully!');
            form.resetFields();
            onClose();
            onSuccess?.(); // Optional success callback
        } catch (error) {
            console.error('Failed to create role:', error);
            message.error('Failed to create role. Please try again.');
        }
    };

    /**
     * Handles modal cancellation, resetting form and closing modal
     */
    const handleCancel = () => {
        form.resetFields();
        onClose();
    };

    return (
        <StyledModal
            title={null}
            visible={visible}
            onCancel={handleCancel}
            footer={[
                <Button key="cancel" onClick={handleCancel}>
                    Cancel
                </Button>,
                <Button key="submit" type="primary" loading={loading} onClick={handleSubmit}>
                    Create Role
                </Button>,
            ]}
            width={600}
        >
            <FormContainer>
                <StyledTitle level={4}>Create new role</StyledTitle>

                <Form form={form} layout="vertical" requiredMark={false}>
                    <Form.Item
                        name="name"
                        label="Role Name"
                        rules={[
                            { required: true, message: 'Please enter a role name' },
                            { min: 1, message: 'Role name cannot be empty' },
                            { max: 100, message: 'Role name must be less than 100 characters' },
                        ]}
                    >
                        <Input placeholder="Enter role name (e.g., Data Analyst, Marketing Manager)" autoFocus />
                    </Form.Item>

                    <Form.Item
                        name="description"
                        label="Description (Optional)"
                        rules={[{ max: 500, message: 'Description must be less than 500 characters' }]}
                    >
                        <TextArea
                            placeholder="Describe the role's purpose and responsibilities (optional)"
                            rows={4}
                            showCount
                            maxLength={500}
                        />
                    </Form.Item>

                    <Divider />

                    <Form.Item name="policyUrns" label="Associated Policies (Optional)">
                        <Select
                            mode="multiple"
                            placeholder="Select policies to associate with this role"
                            loading={policiesLoading}
                            allowClear
                            showSearch
                            filterOption={(input, option) =>
                                (option?.label ?? '').toLowerCase().includes(input.toLowerCase())
                            }
                            options={
                                policiesData?.listPolicies?.policies
                                    ?.filter((policy) => policy.state !== 'INACTIVE')
                                    ?.map((policy) => {
                                        const isImmutable = policy.editable === false;
                                        const isPlatformPolicy =
                                            (policy.type as string)?.toUpperCase() === 'PLATFORM' ||
                                            policy.name?.toLowerCase().includes('platform');
                                        const policyTypeLabel = isPlatformPolicy ? 'Platform' : 'Metadata';

                                        return {
                                            label: `${policy.name} (${policyTypeLabel})`,
                                            value: policy.urn,
                                            disabled: isImmutable,
                                            title: isImmutable
                                                ? 'System policies cannot be associated with custom roles. Only editable policies can be assigned to custom roles.'
                                                : undefined,
                                        };
                                    })
                                    ?.sort((a, b) => a.label.localeCompare(b.label)) || []
                            }
                        />
                        <Typography.Text
                            type="secondary"
                            style={{ fontSize: '12px', display: 'block', marginTop: '4px' }}
                        >
                            Associate existing policies with this role. Users assigned to this role will inherit these
                            policy privileges. Only editable policies can be associated with custom roles.
                        </Typography.Text>
                    </Form.Item>
                </Form>

                <Typography.Text type="secondary">
                    You can also manage policy associations later from the role details page.
                </Typography.Text>
            </FormContainer>
        </StyledModal>
    );
};
