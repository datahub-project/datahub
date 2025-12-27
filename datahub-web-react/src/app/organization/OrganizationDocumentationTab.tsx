import React from 'react';
import { Form, Input, Button, message } from 'antd';
import styled from 'styled-components';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { useUpdateOrganizationMutation } from '@graphql/organization.generated';
import { Organization, EntityType } from '@types';
import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';

const { TextArea } = Input;

const Container = styled.div`
    padding: 20px;
`;

const FormSection = styled.div`
    margin-bottom: 24px;
    background: white;
    padding: 20px;
    border-radius: 8px;
    border: 1px solid #f0f0f0;
`;

const SectionTitle = styled.h3`
    margin-bottom: 16px;
    font-size: 16px;
    font-weight: 600;
`;

export const OrganizationDocumentationTab = () => {
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const organization = entityData as Organization;
    const [updateOrganization, { loading }] = useUpdateOrganizationMutation();
    const [form] = Form.useForm();

    const initialValues = {
        name: organization?.properties?.name || '',
        description: organization?.properties?.description || '',
        logoUrl: organization?.properties?.logoUrl || '',
        parentUrn: organization?.parent?.urn || null,
    };

    const handleSubmit = async (values: any) => {
        try {
            await updateOrganization({
                variables: {
                    urn: organization.urn,
                    input: {
                        name: values.name,
                        description: values.description || null,
                        logoUrl: values.logoUrl || null,
                        parentUrn: values.parentUrn || null,
                    },
                },
            });

            message.success('Organization updated successfully!');
            refetch?.();
        } catch (e: any) {
            message.error(`Failed to update organization: ${e.message || 'Unknown error'}`);
        }
    };

    return (
        <Container>
            <Form
                form={form}
                layout="vertical"
                initialValues={initialValues}
                onFinish={handleSubmit}
            >
                <FormSection>
                    <SectionTitle>Basic Information</SectionTitle>

                    <Form.Item
                        name="name"
                        label="Organization Name"
                        rules={[
                            { required: true, message: 'Please enter an organization name' },
                            { max: 100, message: 'Name cannot exceed 100 characters' },
                        ]}
                    >
                        <Input
                            placeholder="Enter organization name"
                            size="large"
                        />
                    </Form.Item>

                    <Form.Item
                        name="description"
                        label="Description"
                        rules={[
                            { max: 500, message: 'Description cannot exceed 500 characters' },
                        ]}
                    >
                        <TextArea
                            rows={4}
                            placeholder="Enter a description for this organization"
                        />
                    </Form.Item>

                    <Form.Item>
                        <Button
                            type="primary"
                            htmlType="submit"
                            loading={loading}
                            size="large"
                        >
                            Save Changes
                        </Button>
                    </Form.Item>
                </FormSection>

                <FormSection>
                    <SectionTitle>Hierarchy</SectionTitle>
                    <Form.Item
                        name="parentUrn"
                        label="Parent Organization"
                        extra="Select the parent organization for this entity."
                    >
                        <EntitySearchSelect
                            entityTypes={[EntityType.Organization]}
                            placeholder="Search for parent organization..."
                            onUpdate={(urns) => form.setFieldsValue({ parentUrn: urns[0] || null })}
                            selectedUrns={form.getFieldValue('parentUrn') ? [form.getFieldValue('parentUrn')] : []}
                            width="full"
                        />
                    </Form.Item>

                    {organization?.children && organization.children.length > 0 && (
                        <div style={{ marginTop: 24 }}>
                            <SectionTitle>Children Organizations</SectionTitle>
                            <ul style={{ paddingLeft: 20 }}>
                                {organization.children.map((child) => (
                                    <li key={child.urn} style={{ marginBottom: 8 }}>
                                        <a href={`/organization/${child.urn}`}>
                                            {child.properties?.name || child.urn}
                                        </a>
                                    </li>
                                ))}
                            </ul>
                        </div>
                    )}
                </FormSection>
            </Form>
        </Container>
    );
};
