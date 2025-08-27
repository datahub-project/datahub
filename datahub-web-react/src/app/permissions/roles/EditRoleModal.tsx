import { Button, Divider, Form, Input, Modal, Select, Typography, message } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { useUpdateRoleMutation } from '@graphql/mutations.generated';
import { useListPoliciesQuery } from '@graphql/policy.generated';
import { DataHubRole, DataHubPolicy, PolicyType } from '@types';

const { TextArea } = Input;
const { Title } = Typography;

/**
 * Modal for editing existing custom roles in DataHub.
 * Allows users to modify role name, description, and policy associations.
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
 * Props for the EditRoleModal component
 */
type Props = {
    /** Whether the modal is visible */
    visible: boolean;
    /** The role to edit */
    role: DataHubRole | null;
    /** Callback when modal is closed */
    onClose: () => void;
    /** Optional callback when role is successfully updated */
    onSuccess?: () => void;
};

/**
 * Form data structure for role editing
 */
interface EditRoleFormData {
    /** Role name (required) */
    name: string;
    /** Role description (optional) */
    description?: string;
    /** Associated policy URNs (optional) */
    policyUrns?: string[];
}

/**
 * EditRoleModal component for editing existing custom roles.
 * Pre-populates form with current role data and handles updates.
 */
export const EditRoleModal = ({ visible, role, onClose, onSuccess }: Props) => {
    const [form] = Form.useForm<EditRoleFormData>();
    const [updateRole, { loading }] = useUpdateRoleMutation();

    // Fetch available policies for association
    const { data: policiesData, loading: policiesLoading } = useListPoliciesQuery({
        variables: {
            input: {
                start: 0,
                count: 100, // Get first 100 policies
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    /**
     * Pre-populate form fields when modal opens with existing role data
     */
    useEffect(() => {
        if (visible && role) {
            // Extract policy URNs from role relationships (using same approach as RoleDetailsModal)
            const castedRole = role as any;
            const policies = castedRole?.policies?.relationships?.map((relationship) => relationship.entity as DataHubPolicy);
            const associatedPolicyUrns = policies?.map(policy => policy.urn).filter(Boolean) || [];

            form.setFieldsValue({
                name: role.name || '',
                description: role.description || '',
                policyUrns: associatedPolicyUrns,
            });
        }
    }, [visible, role, form]);

    /**
     * Handles role update form submission with validation and error handling
     */
    const handleSubmit = async () => {
        if (!role) return;

        try {
            const values = await form.validateFields();

            const result = await updateRole({
                variables: {
                    urn: role.urn,
                    input: {
                        name: values.name.trim(),
                        description: values.description?.trim() || '', // Optional description
                        policyUrns: values.policyUrns || [],
                    },
                },
            });

            message.success('Role updated successfully!');
            onClose();
            onSuccess?.(); // Optional success callback
        } catch (error) {
            console.error('Failed to update role:', error);
            message.error('Failed to update role. Please try again.');
        }
    };

    /**
     * Handles modal cancellation, resetting form and closing modal
     */
    const handleCancel = () => {
        form.resetFields();
        onClose();
    };

    // Don't render if no role is provided
    if (!role) return null;

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
                    Update Role
                </Button>,
            ]}
            width={600}
        >
            <FormContainer>
                <StyledTitle level={4}>Edit Role</StyledTitle>

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
                        <Input placeholder="Enter role name (e.g., Data Analyst, Marketing Manager)" />
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

                    <Form.Item name="policyUrns" label="Associated Policies">
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
                            Users assigned to this role will inherit privileges from these policies. Only editable policies can be associated with custom roles.
                        </Typography.Text>
                    </Form.Item>
                </Form>

                <Typography.Text type="secondary">
                    Changes to policy associations will take effect immediately for all users with this role.
                </Typography.Text>
            </FormContainer>
        </StyledModal>
    );
};
