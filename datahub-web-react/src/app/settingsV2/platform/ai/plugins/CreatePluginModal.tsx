import { CaretDown, CaretRight, CheckCircle, Trash } from '@phosphor-icons/react';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { usePluginForm } from '@app/settingsV2/platform/ai/plugins/hooks/usePluginForm';
import { Button, Checkbox, Input, Modal, SimpleSelect, Text, TextArea, colors } from '@src/alchemy-components';

import { AiPluginAuthType, AiPluginConfig } from '@types';

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

const CallbackUrlContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px;
    background: ${colors.gray[1500]};
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
`;

const CallbackUrlText = styled.code`
    flex: 1;
    font-size: 13px;
    color: ${colors.gray[600]};
    word-break: break-all;
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

const SecretStatus = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: #52c41a;
    margin-bottom: 4px;
`;

// Constants
const AUTH_TYPE_OPTIONS = [
    { label: 'None (Public API)', value: AiPluginAuthType.None },
    { label: 'Shared API Key (System-wide)', value: AiPluginAuthType.SharedApiKey },
    { label: 'User API Key (Each user provides their own)', value: AiPluginAuthType.UserApiKey },
    { label: 'User OAuth (Each user authenticates)', value: AiPluginAuthType.UserOauth },
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

    // Generate the OAuth callback URL based on current environment
    const callbackUrl = useMemo(() => {
        return `${window.location.origin}/integrations/oauth/callback`;
    }, []);

    const [copied, setCopied] = useState(false);
    const [showOAuthAdvanced, setShowOAuthAdvanced] = useState(false);

    const handleCopyCallbackUrl = () => {
        navigator.clipboard.writeText(callbackUrl);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    const getOAuthClientSecretPlaceholder = () => {
        if (isEditing && formState.hasOAuthClientSecret) {
            return 'Enter new secret to replace existing';
        }
        if (isEditing) {
            return 'Enter client secret';
        }
        return 'OAuth Client Secret';
    };

    return (
        <Modal
            title={getModalTitle()}
            subtitle="Add an MCP server that can be accessed by Ask DataHub"
            onCancel={onClose}
            width={600}
            maskClosable={false}
            data-testid="create-plugin-modal"
            footer={
                <ModalFooter>
                    <Checkbox
                        label="Enable for Ask DataHub"
                        isChecked={formState.enabled}
                        setIsChecked={(checked) => updateField('enabled', checked)}
                        shouldHandleLabelClicks
                        data-testid="plugin-enabled-checkbox"
                    />
                    <FooterButtons>
                        <Button variant="outline" onClick={onClose} data-testid="plugin-cancel-button">
                            Cancel
                        </Button>
                        <Button
                            variant="filled"
                            onClick={handleSubmit}
                            isLoading={isSaving}
                            data-testid="plugin-submit-button"
                        >
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
                            placeholder="Glean MCP Server"
                            value={formState.displayName}
                            setValue={(val) => updateField('displayName', val)}
                            isRequired
                            error={errors.displayName}
                            data-testid="plugin-name-input"
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
                            data-testid="plugin-url-input"
                            helperText="stdio-based MCP plugins are not currently supported."
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
                            {/* Callback URL Info */}
                            <FormField>
                                <Text size="sm" weight="semiBold" style={{ marginBottom: 8, display: 'block' }}>
                                    OAuth Callback URL
                                </Text>
                                <CallbackUrlContainer>
                                    <CallbackUrlText>{callbackUrl}</CallbackUrlText>
                                    <Button
                                        variant="text"
                                        size="sm"
                                        onClick={handleCopyCallbackUrl}
                                        icon={{ icon: copied ? 'Check' : 'ContentCopy', source: 'material' }}
                                    >
                                        {copied ? 'Copied' : 'Copy'}
                                    </Button>
                                </CallbackUrlContainer>
                                <Text size="sm" color="gray" style={{ marginTop: 4 }}>
                                    Use this URL when registering your OAuth application with the provider.
                                </Text>
                            </FormField>

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
                                        placeholder="Glean OAuth"
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
                                    {isEditing && formState.hasOAuthClientSecret && (
                                        <SecretStatus>
                                            <CheckCircle size={14} weight="fill" /> Client secret is configured. Enter
                                            new value to replace.
                                        </SecretStatus>
                                    )}
                                    <Input
                                        label="Client Secret"
                                        placeholder={getOAuthClientSecretPlaceholder()}
                                        value={formState.oauthClientSecret}
                                        setValue={(val) => updateField('oauthClientSecret', val)}
                                        isPassword
                                        isRequired={!isEditing || !formState.hasOAuthClientSecret}
                                        helperText={
                                            isEditing && formState.hasOAuthClientSecret
                                                ? 'Leave empty to keep existing secret, or enter a new value to replace it.'
                                                : 'Will be securely encrypted'
                                        }
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
                                        placeholder="openid, profile, repo, user"
                                        value={formState.oauthScopes}
                                        setValue={(val) => updateField('oauthScopes', val)}
                                        helperText="Comma-separated scopes requested from the OAuth provider."
                                    />
                                </FormField>

                                {/* OAuth Advanced Settings */}
                                <AdvancedToggle
                                    onClick={() => setShowOAuthAdvanced(!showOAuthAdvanced)}
                                    style={{ marginTop: 16, marginBottom: 0 }}
                                >
                                    {showOAuthAdvanced ? <CaretDown size={16} /> : <CaretRight size={16} />}
                                    <Text weight="semiBold">Advanced OAuth Settings</Text>
                                </AdvancedToggle>

                                {showOAuthAdvanced && (
                                    <>
                                        <FormField style={{ marginTop: 12 }}>
                                            <Text
                                                weight="semiBold"
                                                style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}
                                            >
                                                Token Auth Method
                                            </Text>
                                            <SimpleSelect
                                                options={[
                                                    { label: 'Basic Auth (default)', value: 'BASIC' },
                                                    { label: 'POST Body', value: 'POST_BODY' },
                                                    { label: 'None (Public Client)', value: 'NONE' },
                                                ]}
                                                values={[formState.oauthTokenAuthMethod]}
                                                onUpdate={(vals) => updateField('oauthTokenAuthMethod', vals[0])}
                                                width="full"
                                            />
                                            <Text color="gray" size="sm" style={{ marginTop: 4 }}>
                                                How to send client credentials when exchanging tokens. Use
                                                &quot;None&quot; for public OAuth clients.
                                            </Text>
                                        </FormField>

                                        <FormField>
                                            <Text
                                                weight="semiBold"
                                                style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}
                                            >
                                                Auth Injection Location
                                            </Text>
                                            <SimpleSelect
                                                options={[
                                                    { label: 'Header (default)', value: 'HEADER' },
                                                    { label: 'Query Parameter', value: 'QUERY_PARAM' },
                                                ]}
                                                values={[formState.oauthAuthLocation]}
                                                onUpdate={(vals) => updateField('oauthAuthLocation', vals[0])}
                                                width="full"
                                            />
                                            <Text color="gray" size="sm" style={{ marginTop: 4 }}>
                                                Where to inject the access token when calling the MCP server.
                                            </Text>
                                        </FormField>

                                        {formState.oauthAuthLocation === 'HEADER' && (
                                            <>
                                                <FormField>
                                                    <Input
                                                        label="Auth Header Name"
                                                        placeholder="Authorization"
                                                        value={formState.oauthAuthHeaderName}
                                                        setValue={(val) => updateField('oauthAuthHeaderName', val)}
                                                        helperText="Header name for the access token (defaults to Authorization)."
                                                    />
                                                </FormField>

                                                <FormField>
                                                    <Input
                                                        label="Auth Scheme"
                                                        placeholder="Bearer"
                                                        value={formState.oauthAuthScheme}
                                                        setValue={(val) => updateField('oauthAuthScheme', val)}
                                                        helperText="Prefix for the token value (defaults to Bearer)."
                                                    />
                                                </FormField>
                                            </>
                                        )}

                                        {formState.oauthAuthLocation === 'QUERY_PARAM' && (
                                            <FormField>
                                                <Input
                                                    label="Query Parameter Name"
                                                    placeholder="access_token"
                                                    value={formState.oauthAuthQueryParam}
                                                    setValue={(val) => updateField('oauthAuthQueryParam', val)}
                                                    helperText="Query parameter name for the access token."
                                                />
                                            </FormField>
                                        )}
                                    </>
                                )}
                            </InlineOAuthForm>
                        </>
                    )}
                </FormSection>

                {/* Instructions */}
                <FormSection>
                    <SectionTitle>LLM Instructions</SectionTitle>

                    <FormField>
                        <TextArea
                            label="Instructions for the AI Assistant"
                            placeholder="Use this plugin to search company documentation and internal wikis..."
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
                            {/* Timeout */}
                            <FormField>
                                <Input
                                    label="Timeout (seconds)"
                                    placeholder="30"
                                    value={formState.timeout}
                                    setValue={(val) => updateField('timeout', val)}
                                    type="number"
                                    helperText="Connection timeout for MCP server requests."
                                />
                            </FormField>

                            {/* Auth Scheme for API Key auth types */}
                            {(formState.authType === AiPluginAuthType.SharedApiKey ||
                                formState.authType === AiPluginAuthType.UserApiKey) && (
                                <FormField>
                                    <Input
                                        label="Auth Scheme"
                                        placeholder="Bearer"
                                        value={
                                            formState.authType === AiPluginAuthType.SharedApiKey
                                                ? formState.sharedApiKeyAuthScheme
                                                : formState.userApiKeyAuthScheme
                                        }
                                        setValue={(val) =>
                                            updateField(
                                                formState.authType === AiPluginAuthType.SharedApiKey
                                                    ? 'sharedApiKeyAuthScheme'
                                                    : 'userApiKeyAuthScheme',
                                                val,
                                            )
                                        }
                                        helperText="Prefix for Authorization header (Bearer, Token, ApiKey)"
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
                                    Additional headers to send with every request (x-dbt-prod-environment-id)
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
