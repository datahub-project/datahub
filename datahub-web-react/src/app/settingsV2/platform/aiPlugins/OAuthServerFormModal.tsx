import { CheckCircleOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { Form, Input, Modal, Select, Tooltip, message } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import {
    useOAuthAuthorizationServerQuery,
    useUpsertOAuthAuthorizationServerMutation,
} from '@graphql/aiPlugins.generated';
import { AuthLocation, TokenAuthMethod } from '@types';

const { TextArea } = Input;

const SecretStatus = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: #52c41a;
    margin-bottom: 4px;
`;

interface OAuthServerFormModalProps {
    editingUrn: string | null;
    onClose: () => void;
}

interface OAuthServerFormValues {
    displayName: string;
    description?: string;
    // OAuth config
    clientId?: string;
    clientSecret?: string;
    authorizationUrl?: string;
    tokenUrl?: string;
    scopes?: string[];
    tokenAuthMethod: TokenAuthMethod;
    // Auth injection
    authLocation: AuthLocation;
    authHeaderName: string;
    authScheme?: string;
}

const OAuthServerFormModal: React.FC<OAuthServerFormModalProps> = ({ editingUrn, onClose }) => {
    const [form] = Form.useForm<OAuthServerFormValues>();
    const isEditing = !!editingUrn;

    const { data: serverData } = useOAuthAuthorizationServerQuery({
        variables: { urn: editingUrn! },
        skip: !editingUrn,
    });

    const [upsertServer, { loading: saving }] = useUpsertOAuthAuthorizationServerMutation();

    // Check if client secret is already configured (actual value is never exposed)
    const hasClientSecret = serverData?.oauthAuthorizationServer?.properties?.hasClientSecret ?? false;

    useEffect(() => {
        if (serverData?.oauthAuthorizationServer) {
            const server = serverData.oauthAuthorizationServer;
            form.setFieldsValue({
                displayName: server.properties?.displayName || '',
                description: server.properties?.description || '',
                clientId: server.properties?.clientId || '',
                authorizationUrl: server.properties?.authorizationUrl || '',
                tokenUrl: server.properties?.tokenUrl || '',
                scopes: server.properties?.scopes || [],
                tokenAuthMethod: server.properties?.tokenAuthMethod || TokenAuthMethod.PostBody,
                authLocation: server.properties?.authLocation || AuthLocation.Header,
                authHeaderName: server.properties?.authHeaderName || 'Authorization',
                authScheme: server.properties?.authScheme || 'Bearer',
                // Don't populate secrets - they're stored securely and not returned
            });
        }
    }, [serverData, form]);

    const handleSubmit = async () => {
        try {
            const values = await form.validateFields();

            await upsertServer({
                variables: {
                    input: {
                        id: editingUrn ? editingUrn.split(':').pop() : undefined,
                        displayName: values.displayName,
                        description: values.description,
                        clientId: values.clientId,
                        // Only send client secret if a new value was entered
                        clientSecret: values.clientSecret || undefined,
                        authorizationUrl: values.authorizationUrl,
                        tokenUrl: values.tokenUrl,
                        scopes: values.scopes,
                        tokenAuthMethod: values.tokenAuthMethod,
                        authLocation: values.authLocation,
                        authHeaderName: values.authHeaderName,
                        authScheme: values.authScheme,
                    },
                },
            });

            message.success(isEditing ? 'OAuth server updated successfully' : 'OAuth server created successfully');
            onClose();
        } catch (error) {
            if (error instanceof Error && !error.message.includes('validation')) {
                message.error('Failed to save OAuth server');
            }
        }
    };

    return (
        <Modal
            title={isEditing ? 'Edit OAuth Authorization Server' : 'Add OAuth Authorization Server'}
            open
            onCancel={onClose}
            onOk={handleSubmit}
            okText={isEditing ? 'Save' : 'Create'}
            confirmLoading={saving}
            width={700}
        >
            <Form
                form={form}
                layout="vertical"
                initialValues={{
                    tokenAuthMethod: TokenAuthMethod.PostBody,
                    authLocation: AuthLocation.Header,
                    authHeaderName: 'Authorization',
                    authScheme: 'Bearer',
                }}
            >
                <Form.Item name="displayName" label="Name" rules={[{ required: true, message: 'Please enter a name' }]}>
                    <Input placeholder="e.g., Glean" />
                </Form.Item>

                <Form.Item name="description" label="Description">
                    <TextArea placeholder="Describe this OAuth provider..." rows={2} />
                </Form.Item>

                {/* OAuth configuration fields */}
                <Form.Item
                    name="clientId"
                    label="Client ID"
                    rules={[{ required: true, message: 'Client ID is required for OAuth' }]}
                >
                    <Input placeholder="OAuth Client ID" />
                </Form.Item>

                <Form.Item
                    name="clientSecret"
                    label={
                        <span>
                            Client Secret{' '}
                            <Tooltip title="OAuth client secret. Will be securely encrypted.">
                                <InfoCircleOutlined />
                            </Tooltip>
                        </span>
                    }
                    rules={[
                        {
                            required: !isEditing || !hasClientSecret,
                            message: 'Client secret is required for OAuth',
                        },
                    ]}
                >
                    {isEditing && hasClientSecret && (
                        <SecretStatus>
                            <CheckCircleOutlined /> Client secret is configured. Enter new value to replace.
                        </SecretStatus>
                    )}
                    <Input.Password
                        placeholder={
                            isEditing && hasClientSecret
                                ? 'Enter new secret to replace existing'
                                : 'Enter OAuth client secret'
                        }
                    />
                </Form.Item>

                <Form.Item
                    name="authorizationUrl"
                    label="Authorization URL"
                    rules={[
                        { required: true, message: 'Authorization URL is required for OAuth' },
                        { type: 'url', message: 'Please enter a valid URL' },
                    ]}
                >
                    <Input placeholder="https://provider.com/oauth/authorize" />
                </Form.Item>

                <Form.Item
                    name="tokenUrl"
                    label="Token URL"
                    rules={[
                        { required: true, message: 'Token URL is required for OAuth' },
                        { type: 'url', message: 'Please enter a valid URL' },
                    ]}
                >
                    <Input placeholder="https://provider.com/oauth/token" />
                </Form.Item>

                <Form.Item name="scopes" label="Default Scopes">
                    <Select mode="tags" placeholder="Enter scopes (e.g., read, write)" />
                </Form.Item>

                <Form.Item name="tokenAuthMethod" label="Token Authentication Method">
                    <Select>
                        <Select.Option value={TokenAuthMethod.PostBody}>POST Body</Select.Option>
                        <Select.Option value={TokenAuthMethod.Basic}>Basic Auth Header</Select.Option>
                        <Select.Option value={TokenAuthMethod.None}>None (Public Client)</Select.Option>
                    </Select>
                </Form.Item>

                {/* Auth injection settings */}
                <Form.Item name="authLocation" label="Token Location">
                    <Select>
                        <Select.Option value={AuthLocation.Header}>Header</Select.Option>
                        <Select.Option value={AuthLocation.QueryParam}>Query Parameter</Select.Option>
                    </Select>
                </Form.Item>

                <Form.Item name="authHeaderName" label="Header Name">
                    <Input placeholder="Authorization" />
                </Form.Item>

                <Form.Item name="authScheme" label="Auth Scheme (prefix)">
                    <Input placeholder="Bearer" />
                </Form.Item>
            </Form>
        </Modal>
    );
};

export default OAuthServerFormModal;
