import { CaretDown, CaretRight, Trash } from '@phosphor-icons/react';
import { Radio } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { usePluginForm } from '@app/settingsV2/platform/aiPlugins/hooks/usePluginForm';
import { Button, Checkbox, Input, Modal, SimpleSelect, Text, TextArea, colors } from '@src/alchemy-components';

import { AiPluginAuthType, AiPluginConfig, McpTransport } from '@types';

interface CreatePluginModalProps {
    editingPlugin: AiPluginConfig | null;
    onClose: () => void;
    existingNames?: string[];
}

// Styled components
const ModalContent = styled.div`
    max-height: 70vh;
    overflow-y: auto;
    padding: 0 4px;
`;

const FormSection = styled.div`
    margin-bottom: 24px;
`;

const SectionTitle = styled.div`
    font-size: 13px;
    font-weight: 600;
    color: ${colors.gray[600]};
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid ${colors.gray[100]};
`;

const FormField = styled.div`
    margin-bottom: 16px;
`;

const InlineOAuthForm = styled.div`
    padding: 16px;
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    background: ${colors.gray[1500]};
    margin-top: 8px;
`;

const StyledRadioGroup = styled(Radio.Group)`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const AdvancedToggle = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
    color: ${colors.gray[1700]};
    font-size: 13px;
    margin-bottom: 16px;

    &:hover {
        color: ${colors.gray[600]};
    }
`;

const HeaderRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: flex-end;
    margin-bottom: 8px;
`;

const HeaderKeyInput = styled.div`
    flex: 1;
`;

const HeaderValueInput = styled.div`
    flex: 1;
`;

const RemoveButton = styled.div`
    padding-bottom: 8px;
    cursor: pointer;
    color: ${colors.red[500]};

    &:hover {
        color: ${colors.red[700]};
    }
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
`;

const FooterButtons = styled.div`
    display: flex;
    gap: 8px;
`;

// Constants
const AUTH_TYPE_OPTIONS = [
    { label: 'None (Public API)', value: AiPluginAuthType.None },
    { label: 'Shared API Key (System-wide)', value: AiPluginAuthType.SharedApiKey },
    { label: 'User API Key (Each user provides their own)', value: AiPluginAuthType.UserApiKey },
    { label: 'User OAuth (Each user authenticates)', value: AiPluginAuthType.UserOauth },
];

const TRANSPORT_OPTIONS = [
    { label: 'HTTP / SSE', value: McpTransport.Http },
    { label: 'WebSocket', value: McpTransport.Websocket },
];

const CreatePluginModal: React.FC<CreatePluginModalProps> = ({ editingPlugin, onClose, existingNames = [] }) => {
    const {
        formState,
        errors,
        showAdvanced,
        isEditing,
        isSaving,
        updateField,
        setShowAdvanced,
        addHeader,
        updateHeader,
        removeHeader,
        handleSubmit,
        getModalTitle,
    } = usePluginForm({
        editingPlugin,
        existingNames,
        onSuccess: onClose,
    });

    return (
        <Modal
            title={getModalTitle()}
            subtitle="Add an MCP server that can be accessed by Ask DataHub"
            onCancel={onClose}
            width={600}
            maskClosable={false}
            footer={
                <ModalFooter>
                    <Checkbox
                        label="Enable for Ask DataHub"
                        isChecked={formState.enabled}
                        setIsChecked={(checked) => updateField('enabled', checked)}
                        shouldHandleLabelClicks
                    />
                    <FooterButtons>
                        <Button variant="outline" onClick={onClose}>
                            Cancel
                        </Button>
                        <Button variant="filled" onClick={handleSubmit} isLoading={isSaving}>
                            {isEditing ? 'Save' : 'Create'}
                        </Button>
                    </FooterButtons>
                </ModalFooter>
            }
        >
            <ModalContent>
                {/* Basic Info */}
                <FormSection>
                    <FormField>
                        <Input
                            label="Name"
                            placeholder="e.g., Glean MCP Server"
                            value={formState.displayName}
                            setValue={(val) => updateField('displayName', val)}
                            isRequired
                            error={errors.displayName}
                        />
                    </FormField>

                    <FormField>
                        <TextArea
                            label="Description"
                            placeholder="Describe what this plugin provides..."
                            value={formState.description}
                            onChange={(e) => updateField('description', e.target.value)}
                            rows={2}
                        />
                    </FormField>
                </FormSection>

                {/* Connection Settings */}
                <FormSection>
                    <SectionTitle>Connection</SectionTitle>

                    <FormField>
                        <Input
                            label="Server URL"
                            placeholder="https://api.example.com/mcp"
                            value={formState.url}
                            setValue={(val) => updateField('url', val)}
                            isRequired
                            error={errors.url}
                        />
                    </FormField>

                    <FormField>
                        <Text weight="semiBold" style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}>
                            Transport
                        </Text>
                        <StyledRadioGroup
                            value={formState.transport}
                            onChange={(e) => updateField('transport', e.target.value)}
                        >
                            {TRANSPORT_OPTIONS.map((opt) => (
                                <Radio key={opt.value} value={opt.value}>
                                    {opt.label}
                                </Radio>
                            ))}
                        </StyledRadioGroup>
                    </FormField>

                    <FormField>
                        <Input
                            label="Timeout (seconds)"
                            placeholder="30"
                            value={formState.timeout}
                            setValue={(val) => updateField('timeout', val)}
                            type="number"
                        />
                    </FormField>
                </FormSection>

                {/* Authentication */}
                <FormSection>
                    <SectionTitle>Authentication</SectionTitle>

                    <FormField>
                        <Text weight="semiBold" style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}>
                            Authentication Type
                        </Text>
                        <SimpleSelect
                            options={AUTH_TYPE_OPTIONS}
                            values={[formState.authType]}
                            onUpdate={(vals) => updateField('authType', vals[0] as AiPluginAuthType)}
                            width="full"
                        />
                    </FormField>

                    {/* Shared API Key */}
                    {formState.authType === AiPluginAuthType.SharedApiKey && (
                        <FormField>
                            <Input
                                label="Shared API Key"
                                placeholder={
                                    isEditing ? 'Enter new API key to replace existing' : 'Enter the shared API key'
                                }
                                value={formState.sharedApiKey}
                                setValue={(val) => updateField('sharedApiKey', val)}
                                isPassword
                                isRequired={!isEditing}
                                error={errors.sharedApiKey}
                                helperText="This key will be used for all users. Will be securely encrypted."
                            />
                        </FormField>
                    )}

                    {/* OAuth Provider Configuration */}
                    {formState.authType === AiPluginAuthType.UserOauth && (
                        <>
                            <InlineOAuthForm>
                                <Text
                                    weight="bold"
                                    style={{ marginBottom: 12, display: 'block', color: colors.gray[600] }}
                                >
                                    Credential Provider
                                </Text>

                                <FormField>
                                    <Input
                                        label="Provider Name"
                                        placeholder="e.g., Glean OAuth"
                                        value={formState.oauthServerName}
                                        setValue={(val) => updateField('oauthServerName', val)}
                                        isRequired
                                        error={errors.oauthServerName}
                                    />
                                </FormField>

                                <FormField>
                                    <Input
                                        label="Description"
                                        placeholder="Description of the provider"
                                        value={formState.oauthServerDescription}
                                        setValue={(val) => updateField('oauthServerDescription', val)}
                                    />
                                </FormField>

                                <FormField>
                                    <Input
                                        label="Client ID"
                                        placeholder="OAuth Client ID"
                                        value={formState.oauthClientId}
                                        setValue={(val) => updateField('oauthClientId', val)}
                                        isRequired
                                        error={errors.oauthClientId}
                                    />
                                </FormField>

                                <FormField>
                                    <Input
                                        label="Client Secret"
                                        placeholder="OAuth Client Secret"
                                        value={formState.oauthClientSecret}
                                        setValue={(val) => updateField('oauthClientSecret', val)}
                                        isPassword
                                        isRequired
                                        error={errors.oauthClientSecret}
                                        helperText="Will be securely encrypted"
                                    />
                                </FormField>

                                <FormField>
                                    <Input
                                        label="Authorization URL"
                                        placeholder="https://provider.com/oauth/authorize"
                                        value={formState.oauthAuthorizationUrl}
                                        setValue={(val) => updateField('oauthAuthorizationUrl', val)}
                                        isRequired
                                        error={errors.oauthAuthorizationUrl}
                                    />
                                </FormField>

                                <FormField>
                                    <Input
                                        label="Token URL"
                                        placeholder="https://provider.com/oauth/token"
                                        value={formState.oauthTokenUrl}
                                        setValue={(val) => updateField('oauthTokenUrl', val)}
                                        isRequired
                                        error={errors.oauthTokenUrl}
                                    />
                                </FormField>

                                <FormField>
                                    <Input
                                        label="Default Scopes"
                                        placeholder="openid, profile, email"
                                        value={formState.oauthScopes}
                                        setValue={(val) => updateField('oauthScopes', val)}
                                        helperText="Comma-separated list of scopes"
                                    />
                                </FormField>
                            </InlineOAuthForm>

                            <FormField style={{ marginTop: 16 }}>
                                <Input
                                    label="Required Scopes for this Plugin"
                                    placeholder="e.g., search:read, documents:read"
                                    value={formState.requiredScopes}
                                    setValue={(val) => updateField('requiredScopes', val)}
                                    helperText="Additional scopes beyond provider defaults"
                                />
                            </FormField>
                        </>
                    )}
                </FormSection>

                {/* Instructions */}
                <FormSection>
                    <SectionTitle>LLM Instructions</SectionTitle>

                    <FormField>
                        <TextArea
                            label="Instructions for the AI Assistant"
                            placeholder="e.g., Use this plugin to search company documentation and internal wikis..."
                            value={formState.instructions}
                            onChange={(e) => updateField('instructions', e.target.value)}
                            rows={4}
                        />
                    </FormField>
                </FormSection>

                {/* Advanced Settings */}
                <FormSection>
                    <AdvancedToggle onClick={() => setShowAdvanced(!showAdvanced)}>
                        {showAdvanced ? <CaretDown size={16} /> : <CaretRight size={16} />}
                        <Text weight="semiBold">Advanced Settings</Text>
                    </AdvancedToggle>

                    {showAdvanced && (
                        <>
                            {/* Auth Scheme for Shared API Key */}
                            {formState.authType === AiPluginAuthType.SharedApiKey && (
                                <FormField>
                                    <Input
                                        label="Auth Scheme"
                                        placeholder="Bearer"
                                        value={formState.sharedApiKeyAuthScheme}
                                        setValue={(val) => updateField('sharedApiKeyAuthScheme', val)}
                                        helperText="Prefix for Authorization header (e.g., Bearer, Token, ApiKey)"
                                    />
                                </FormField>
                            )}

                            {/* Custom Headers */}
                            <FormField>
                                <Text
                                    weight="semiBold"
                                    style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}
                                >
                                    Custom Headers
                                </Text>
                                <Text color="gray" size="sm" style={{ marginBottom: 12, display: 'block' }}>
                                    Additional headers to send with every request (e.g., x-dbt-prod-environment-id)
                                </Text>

                                {formState.customHeaders.map((header, index) => (
                                    <HeaderRow key={header.id}>
                                        <HeaderKeyInput>
                                            <Input
                                                label={index === 0 ? 'Header Name' : ''}
                                                placeholder="x-custom-header"
                                                value={header.key}
                                                setValue={(val) => updateHeader(header.id, 'key', val)}
                                            />
                                        </HeaderKeyInput>
                                        <HeaderValueInput>
                                            <Input
                                                label={index === 0 ? 'Value' : ''}
                                                placeholder="header-value"
                                                value={header.value}
                                                setValue={(val) => updateHeader(header.id, 'value', val)}
                                            />
                                        </HeaderValueInput>
                                        <RemoveButton onClick={() => removeHeader(header.id)}>
                                            <Trash size={18} />
                                        </RemoveButton>
                                    </HeaderRow>
                                ))}

                                <Button
                                    variant="text"
                                    size="sm"
                                    onClick={addHeader}
                                    icon={{ icon: 'Plus', source: 'phosphor' }}
                                >
                                    Add Header
                                </Button>
                            </FormField>
                        </>
                    )}
                </FormSection>
            </ModalContent>
        </Modal>
    );
};

export default CreatePluginModal;
