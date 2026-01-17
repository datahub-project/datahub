import {
    Checkbox,
    Divider,
    Form,
    Input,
    InputNumber,
    Modal,
    Radio,
    RadioChangeEvent,
    Select,
    Space,
    Typography,
    message,
} from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import {
    useListOAuthAuthorizationServersQuery,
    useServiceQuery,
    useUpsertServiceMutation,
} from '@graphql/aiPlugins.generated';
import { AiPluginAuthType, AiPluginConfig, McpTransport, ServiceSubType } from '@types';

const { TextArea } = Input;
const { Text } = Typography;

const SectionDivider = styled(Divider)`
    margin: 16px 0 12px 0;
    font-size: 13px;
`;

const InlineOAuthForm = styled.div`
    padding: 16px;
    border: 1px solid #e8e8e8;
    border-radius: 8px;
    background: #fafafa;
    margin-top: 8px;
`;

interface ServiceFormModalProps {
    /**
     * The AI plugin config being edited, or null for creating a new one.
     * Contains both service reference and AI-specific settings (authType, instructions, etc.)
     */
    editingPlugin: AiPluginConfig | null;
    onClose: () => void;
}

interface ServiceFormValues {
    id?: string;
    displayName: string;
    description?: string;
    url: string;
    transport: McpTransport;
    timeout: number;
    // AI Plugin config
    enabled: boolean;
    instructions?: string;
    authType: AiPluginAuthType;
    // Shared API key (for SHARED_API_KEY auth type)
    sharedApiKey?: string;
    // OAuth provider - either existing or new
    oauthProviderMode: 'existing' | 'new';
    existingOAuthServerUrn?: string;
    // New OAuth server fields
    newOAuthServerName?: string;
    newOAuthServerDescription?: string;
    newOAuthClientId?: string;
    newOAuthClientSecret?: string;
    newOAuthAuthorizationUrl?: string;
    newOAuthTokenUrl?: string;
    newOAuthScopes?: string;
    // Required scopes for this plugin
    requiredScopes?: string;
}

const ServiceFormModal: React.FC<ServiceFormModalProps> = ({ editingPlugin, onClose }) => {
    const [form] = Form.useForm<ServiceFormValues>();
    const isEditing = !!editingPlugin;
    const editingUrn = editingPlugin?.service?.urn || null;

    const [authType, setAuthType] = useState<AiPluginAuthType>(AiPluginAuthType.None);
    const [oauthProviderMode, setOAuthProviderMode] = useState<'existing' | 'new'>('existing');

    // Fetch service details if editing and we don't have full data
    const { data: serviceData } = useServiceQuery({
        variables: { urn: editingUrn! },
        skip: !editingUrn || !!editingPlugin?.service?.mcpServerProperties,
    });

    // Fetch existing OAuth servers for dropdown
    const { data: oauthServersData } = useListOAuthAuthorizationServersQuery({
        variables: { input: { count: 100 } },
    });

    const [upsertService, { loading: saving }] = useUpsertServiceMutation();

    const oauthServers = oauthServersData?.listOAuthAuthorizationServers?.authorizationServers || [];

    // Populate form when editing
    useEffect(() => {
        if (editingPlugin) {
            // Get service data from plugin or separate query
            const service = editingPlugin.service || serviceData?.service;

            // Set auth type state
            const pluginAuthType = editingPlugin.authType || AiPluginAuthType.None;
            setAuthType(pluginAuthType);

            // Get OAuth server URN from oauthConfig (only for USER_OAUTH auth type)
            const oauthServerUrn = editingPlugin.oauthConfig?.serverUrn;
            const hasExistingOAuth = !!oauthServerUrn;
            setOAuthProviderMode(hasExistingOAuth ? 'existing' : 'new');

            // Get required scopes from oauthConfig
            const requiredScopes = editingPlugin.oauthConfig?.requiredScopes;

            form.setFieldsValue({
                displayName: service?.properties?.displayName || '',
                description: service?.properties?.description || '',
                url: service?.mcpServerProperties?.url || '',
                transport: service?.mcpServerProperties?.transport || McpTransport.Http,
                timeout: service?.mcpServerProperties?.timeout || 30,
                // AI Plugin config from the plugin
                enabled: editingPlugin.enabled ?? true,
                instructions: editingPlugin.instructions || '',
                authType: pluginAuthType,
                oauthProviderMode: hasExistingOAuth ? 'existing' : 'new',
                existingOAuthServerUrn: oauthServerUrn || undefined,
                requiredScopes: requiredScopes?.join(', ') || '',
            });
        }
    }, [editingPlugin, serviceData, form]);

    const handleAuthTypeChange = (value: AiPluginAuthType) => {
        setAuthType(value);
        form.setFieldValue('authType', value);
    };

    const handleOAuthModeChange = (e: RadioChangeEvent) => {
        const value = e.target.value as 'existing' | 'new';
        setOAuthProviderMode(value);
        form.setFieldValue('oauthProviderMode', value);
    };

    const handleSubmit = async () => {
        try {
            const values = await form.validateFields();

            // Build OAuth server input if creating new
            let newOAuthServer: any;
            // Only USER_OAUTH requires an OAuth Authorization Server
            const needsOAuthServer = values.authType === AiPluginAuthType.UserOauth;

            if (needsOAuthServer && values.oauthProviderMode === 'new') {
                newOAuthServer = {
                    displayName: values.newOAuthServerName,
                    description: values.newOAuthServerDescription,
                    clientId: values.newOAuthClientId,
                    clientSecret: values.newOAuthClientSecret,
                    authorizationUrl: values.newOAuthAuthorizationUrl,
                    tokenUrl: values.newOAuthTokenUrl,
                    scopes: values.newOAuthScopes ? values.newOAuthScopes.split(',').map((s) => s.trim()) : undefined,
                };
            }

            await upsertService({
                variables: {
                    input: {
                        // Use existing service ID if editing
                        id: editingUrn ? editingUrn.split(':').pop() : undefined,
                        displayName: values.displayName,
                        description: values.description,
                        subType: ServiceSubType.McpServer,
                        mcpServerProperties: {
                            url: values.url,
                            transport: values.transport,
                            timeout: values.timeout,
                        },
                        // AI Plugin config
                        enabled: values.enabled,
                        instructions: values.instructions || undefined,
                        authType: values.authType,
                        oauthServerUrn:
                            needsOAuthServer && values.oauthProviderMode === 'existing'
                                ? values.existingOAuthServerUrn
                                : undefined,
                        newOAuthServer,
                        // Shared API key for SHARED_API_KEY auth type
                        sharedApiKey:
                            values.authType === AiPluginAuthType.SharedApiKey
                                ? values.sharedApiKey || undefined
                                : undefined,
                        requiredScopes: values.requiredScopes
                            ? values.requiredScopes.split(',').map((s) => s.trim())
                            : undefined,
                    },
                },
            });

            message.success(isEditing ? 'MCP server updated successfully' : 'MCP server created successfully');
            onClose();
        } catch (error) {
            if (error instanceof Error && !error.message.includes('validation')) {
                message.error('Failed to save MCP server');
                console.error('Error saving MCP server:', error);
            }
        }
    };

    // Only USER_OAUTH requires an OAuth Authorization Server
    const needsOAuthProvider = authType === AiPluginAuthType.UserOauth;

    const showOAuthFields = authType === AiPluginAuthType.UserOauth;

    return (
        <Modal
            title={isEditing ? 'Edit MCP Server' : 'Add MCP Server'}
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
                    transport: McpTransport.Http,
                    timeout: 30,
                    enabled: true,
                    authType: AiPluginAuthType.None,
                    oauthProviderMode: 'existing',
                }}
            >
                {/* Basic Info */}
                <Form.Item name="displayName" label="Name" rules={[{ required: true, message: 'Please enter a name' }]}>
                    <Input placeholder="e.g., Glean MCP Server" />
                </Form.Item>

                <Form.Item name="description" label="Description">
                    <TextArea placeholder="Describe what this MCP server provides..." rows={2} />
                </Form.Item>

                {/* Connection Settings */}
                <SectionDivider orientation="left">Connection</SectionDivider>

                <Form.Item
                    name="url"
                    label="Server URL"
                    rules={[
                        { required: true, message: 'Please enter the server URL' },
                        { type: 'url', message: 'Please enter a valid URL' },
                    ]}
                >
                    <Input placeholder="https://api.example.com/mcp" />
                </Form.Item>

                <Space style={{ width: '100%' }} size="large">
                    <Form.Item name="transport" label="Transport" style={{ marginBottom: 0, width: 200 }}>
                        <Select>
                            <Select.Option value={McpTransport.Http}>HTTP</Select.Option>
                            <Select.Option value={McpTransport.Sse}>SSE</Select.Option>
                            <Select.Option value={McpTransport.Websocket}>WebSocket</Select.Option>
                        </Select>
                    </Form.Item>

                    <Form.Item
                        name="timeout"
                        label="Timeout (seconds)"
                        style={{ marginBottom: 0, width: 150 }}
                        rules={[{ required: true, message: 'Required' }]}
                    >
                        <InputNumber min={1} max={300} style={{ width: '100%' }} />
                    </Form.Item>
                </Space>

                {/* Authentication */}
                <SectionDivider orientation="left">Authentication</SectionDivider>

                <Form.Item name="authType" label="Authentication Type">
                    <Select onChange={handleAuthTypeChange}>
                        <Select.Option value={AiPluginAuthType.None}>None (Public API)</Select.Option>
                        <Select.Option value={AiPluginAuthType.SharedApiKey}>
                            Shared API Key (System-wide)
                        </Select.Option>
                        <Select.Option value={AiPluginAuthType.UserApiKey}>
                            User API Key (Each user provides their own)
                        </Select.Option>
                        <Select.Option value={AiPluginAuthType.UserOauth}>
                            User OAuth (Each user authenticates)
                        </Select.Option>
                    </Select>
                </Form.Item>

                {/* Shared API Key input - shown only for SHARED_API_KEY auth type */}
                {authType === AiPluginAuthType.SharedApiKey && (
                    <Form.Item
                        name="sharedApiKey"
                        label="Shared API Key"
                        extra={
                            isEditing && editingPlugin?.sharedApiKeyConfig?.credentialUrn
                                ? 'A shared API key is already configured. Enter a new value to replace it.'
                                : 'This key will be used for all users. Will be securely encrypted.'
                        }
                        rules={[
                            {
                                required: !isEditing || !editingPlugin?.sharedApiKeyConfig?.credentialUrn,
                                message: 'Please enter the shared API key',
                            },
                        ]}
                    >
                        <Input.Password
                            placeholder={
                                isEditing && editingPlugin?.sharedApiKeyConfig?.credentialUrn
                                    ? 'Enter new API key to replace existing'
                                    : 'Enter the shared API key'
                            }
                        />
                    </Form.Item>
                )}

                {needsOAuthProvider && (
                    <>
                        <Form.Item name="oauthProviderMode" label="Credential Provider">
                            <Radio.Group onChange={handleOAuthModeChange}>
                                <Radio value="existing">Use existing provider</Radio>
                                <Radio value="new">Create new provider</Radio>
                            </Radio.Group>
                        </Form.Item>

                        {oauthProviderMode === 'existing' && (
                            <Form.Item
                                name="existingOAuthServerUrn"
                                label="Select Provider"
                                rules={[{ required: true, message: 'Please select a provider' }]}
                            >
                                <Select placeholder="Select an OAuth/API key provider">
                                    {oauthServers.map((server: any) => (
                                        <Select.Option key={server.urn} value={server.urn}>
                                            {server.properties?.displayName || server.urn}
                                        </Select.Option>
                                    ))}
                                </Select>
                            </Form.Item>
                        )}

                        {oauthProviderMode === 'new' && (
                            <InlineOAuthForm>
                                <Text strong style={{ fontSize: 13 }}>
                                    New Credential Provider
                                </Text>

                                <Form.Item
                                    name="newOAuthServerName"
                                    label="Provider Name"
                                    rules={[{ required: true, message: 'Required' }]}
                                    style={{ marginTop: 12 }}
                                >
                                    <Input placeholder="e.g., Glean OAuth" />
                                </Form.Item>

                                <Form.Item name="newOAuthServerDescription" label="Description">
                                    <Input placeholder="Description of the provider" />
                                </Form.Item>

                                {showOAuthFields && (
                                    <>
                                        <Form.Item
                                            name="newOAuthClientId"
                                            label="Client ID"
                                            rules={[{ required: true, message: 'Required for OAuth' }]}
                                        >
                                            <Input placeholder="OAuth Client ID" />
                                        </Form.Item>

                                        <Form.Item
                                            name="newOAuthClientSecret"
                                            label="Client Secret"
                                            extra="Will be securely encrypted"
                                            rules={[{ required: true, message: 'Client secret is required for OAuth' }]}
                                        >
                                            <Input.Password placeholder="OAuth Client Secret" />
                                        </Form.Item>

                                        <Form.Item
                                            name="newOAuthAuthorizationUrl"
                                            label="Authorization URL"
                                            rules={[
                                                { required: true, message: 'Required for OAuth' },
                                                { type: 'url', message: 'Must be a valid URL' },
                                            ]}
                                        >
                                            <Input placeholder="https://provider.com/oauth/authorize" />
                                        </Form.Item>

                                        <Form.Item
                                            name="newOAuthTokenUrl"
                                            label="Token URL"
                                            rules={[
                                                { required: true, message: 'Required for OAuth' },
                                                { type: 'url', message: 'Must be a valid URL' },
                                            ]}
                                        >
                                            <Input placeholder="https://provider.com/oauth/token" />
                                        </Form.Item>

                                        <Form.Item
                                            name="newOAuthScopes"
                                            label="Default Scopes"
                                            extra="Comma-separated list of scopes"
                                        >
                                            <Input placeholder="openid, profile, email" />
                                        </Form.Item>
                                    </>
                                )}
                            </InlineOAuthForm>
                        )}

                        {authType === AiPluginAuthType.UserOauth && (
                            <Form.Item
                                name="requiredScopes"
                                label="Required Scopes for this Plugin"
                                extra="Additional scopes beyond provider defaults"
                                style={{ marginTop: 16 }}
                            >
                                <Input placeholder="e.g., search:read, documents:read" />
                            </Form.Item>
                        )}
                    </>
                )}

                {/* LLM Instructions */}
                <SectionDivider orientation="left">LLM Instructions</SectionDivider>

                <Form.Item
                    name="instructions"
                    label="Instructions for the AI Assistant"
                    extra="Tell the AI when and how to use this plugin"
                >
                    <TextArea
                        placeholder="e.g., Use this plugin to search company documentation and internal wikis. Prefer this over web search for questions about internal processes, policies, or company-specific information."
                        rows={4}
                    />
                </Form.Item>

                {/* Enable/Disable */}
                <Form.Item name="enabled" valuePropName="checked" style={{ marginBottom: 0 }}>
                    <Checkbox>Enable for Ask DataHub</Checkbox>
                </Form.Item>
            </Form>
        </Modal>
    );
};

export default ServiceFormModal;
