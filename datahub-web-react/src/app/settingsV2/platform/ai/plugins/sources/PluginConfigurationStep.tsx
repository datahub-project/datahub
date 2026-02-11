import { CaretDown, CaretRight, CheckCircle } from '@phosphor-icons/react';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { CustomHeadersEditor } from '@app/settingsV2/platform/ai/plugins/sources/CustomHeadersEditor';
import { OAuthAdvancedFields } from '@app/settingsV2/platform/ai/plugins/sources/OAuthAdvancedFields';
import { StructuredHeadersSection } from '@app/settingsV2/platform/ai/plugins/sources/StructuredHeadersSection';
import { getOverride, shouldShowField } from '@app/settingsV2/platform/ai/plugins/sources/pluginSourceUtils';
import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import { PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';
import { ValidationErrors } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormValidation';
import { Button, Input, SimpleSelect, Text, TextArea, colors } from '@src/alchemy-components';

import { AiPluginAuthType } from '@types';

// ---------------------------------------------------------------------------
// Styled components
// ---------------------------------------------------------------------------

const FormSection = styled.div`
    margin-bottom: 24px;
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

const SecretStatus = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: ${colors.green[500]};
    margin-bottom: 4px;
`;

const CardHeaderRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
`;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ALL_AUTH_TYPE_OPTIONS = [
    { label: 'None (Public API)', value: AiPluginAuthType.None },
    { label: 'Shared API Key (System-wide)', value: AiPluginAuthType.SharedApiKey },
    { label: 'User API Key (Each user provides their own)', value: AiPluginAuthType.UserApiKey },
    { label: 'User OAuth (Each user authenticates)', value: AiPluginAuthType.UserOauth },
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PluginConfigurationStepProps {
    sourceConfig: PluginSourceConfig;
    formState: PluginFormState;
    errors: ValidationErrors;
    isEditing: boolean;
    showAdvanced: boolean;
    setShowAdvanced: (show: boolean) => void;
    updateField: <K extends keyof PluginFormState>(field: K, value: PluginFormState[K]) => void;
    addHeader: () => void;
    updateHeader: (headerId: string, field: 'key' | 'value', value: string) => void;
    removeHeader: (headerId: string) => void;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

/**
 * Step 2 of the plugin creation wizard.
 * Renders a dynamic form driven by the selected PluginSourceConfig.
 * Only fields listed in `visibleFields` and `advancedFields` are rendered.
 */
export const PluginConfigurationStep: React.FC<PluginConfigurationStepProps> = ({
    sourceConfig,
    formState,
    errors,
    isEditing,
    showAdvanced,
    setShowAdvanced,
    updateField,
    addHeader,
    updateHeader,
    removeHeader,
}) => {
    const callbackUrl = useMemo(() => `${window.location.origin}/integrations/oauth/callback`, []);
    const [copied, setCopied] = useState(false);
    const [showOAuthAdvanced, setShowOAuthAdvanced] = useState(false);

    // Pre-compute field overrides to avoid repeated lookups during render
    const overrides = useMemo(
        () => ({
            displayName: getOverride(sourceConfig, 'displayName'),
            description: getOverride(sourceConfig, 'description'),
            url: getOverride(sourceConfig, 'url'),
            sharedApiKey: getOverride(sourceConfig, 'sharedApiKey'),
            oauthServerName: getOverride(sourceConfig, 'oauthServerName'),
            oauthAuthorizationUrl: getOverride(sourceConfig, 'oauthAuthorizationUrl'),
            oauthTokenUrl: getOverride(sourceConfig, 'oauthTokenUrl'),
            oauthScopes: getOverride(sourceConfig, 'oauthScopes'),
            customHeaders: getOverride(sourceConfig, 'customHeaders'),
        }),
        [sourceConfig],
    );

    const handleCopyCallbackUrl = () => {
        navigator.clipboard.writeText(callbackUrl);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    const isCustom = sourceConfig.name === 'custom';
    const showAuthSelector = sourceConfig.allowedAuthTypes.length > 1;
    const { structuredHeaders } = sourceConfig;

    // Filter the full auth options list to only what this source allows
    const authTypeOptions = ALL_AUTH_TYPE_OPTIONS.filter((opt) =>
        sourceConfig.allowedAuthTypes.includes(opt.value as AiPluginAuthType),
    );

    // Helper to check if a field is visible in the main form
    const isVisible = (field: keyof PluginFormState) =>
        shouldShowField(field, sourceConfig.visibleFields, sourceConfig, formState.authType);

    // Helper to check if a field is in the advanced section
    const isAdvanced = (field: keyof PluginFormState) =>
        shouldShowField(field, sourceConfig.advancedFields, sourceConfig, formState.authType);

    // Structured header value helpers
    const handleHeaderValueChange = (headerKey: string, value: string) => {
        updateField('structuredHeaderValues', {
            ...formState.structuredHeaderValues,
            [headerKey]: value,
        });
    };

    // Determine whether the OAuth section should be shown
    const showOAuth =
        formState.authType === AiPluginAuthType.UserOauth &&
        (isVisible('oauthClientId') || isVisible('oauthServerName'));

    // Check if there are any advanced fields at all
    const hasAdvanced = sourceConfig.advancedFields.length > 0;

    return (
        <div data-testid="plugin-configuration-step">
            {/* ---- Basic Info ---- */}
            <FormSection>
                {isVisible('displayName') && (
                    <FormField>
                        <Input
                            label={overrides.displayName.label ?? 'Name'}
                            placeholder={overrides.displayName.placeholder ?? 'Plugin name'}
                            value={formState.displayName}
                            setValue={(val) => updateField('displayName', val)}
                            isRequired
                            error={errors.displayName}
                            data-testid="plugin-name-input"
                        />
                    </FormField>
                )}

                {isVisible('description') && (
                    <FormField>
                        <TextArea
                            label={overrides.description.label ?? 'Description'}
                            placeholder={overrides.description.placeholder ?? 'Describe what this plugin provides...'}
                            value={formState.description}
                            onChange={(e) => updateField('description', e.target.value)}
                            rows={2}
                        />
                    </FormField>
                )}

                {isVisible('url') && (
                    <FormField>
                        <Input
                            label={overrides.url.label ?? 'MCP Server URL'}
                            placeholder={overrides.url.placeholder ?? 'https://api.example.com/mcp'}
                            value={formState.url}
                            setValue={(val) => updateField('url', val)}
                            isRequired
                            error={errors.url}
                            data-testid="plugin-url-input"
                            helperText={
                                overrides.url.helperDocsUrl
                                    ? undefined
                                    : (overrides.url.helperText ??
                                      'stdio-based MCP plugins are not currently supported.')
                            }
                        />
                        {overrides.url.helperDocsUrl && (
                            <Text color="gray" size="sm" style={{ marginTop: 4 }}>
                                {overrides.url.helperText}{' '}
                                <a
                                    href={overrides.url.helperDocsUrl}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    style={{ color: colors.violet[500] }}
                                >
                                    {overrides.url.helperDocsLabel ?? 'Learn more'} ↗
                                </a>
                            </Text>
                        )}
                    </FormField>
                )}
            </FormSection>

            {/* ---- Authentication type selector (hidden when only one auth type is allowed) ---- */}
            {showAuthSelector && (
                <FormSection>
                    <FormField>
                        <Text weight="semiBold" style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}>
                            Authentication Type
                        </Text>
                        <SimpleSelect
                            options={authTypeOptions}
                            values={[formState.authType]}
                            onUpdate={(vals) => updateField('authType', vals[0] as AiPluginAuthType)}
                            showClear={false}
                            width="full"
                        />
                    </FormField>
                </FormSection>
            )}

            {/* ---- Shared API Key ---- */}
            {isVisible('sharedApiKey') && formState.authType === AiPluginAuthType.SharedApiKey && (
                <FormSection>
                    <InlineOAuthForm>
                        <CardHeaderRow>
                            <Text weight="bold" style={{ color: colors.gray[600] }}>
                                {structuredHeaders ? structuredHeaders.sectionTitle : 'API Key'}
                            </Text>
                            {sourceConfig.docsUrl && (
                                <a href={sourceConfig.docsUrl} target="_blank" rel="noopener noreferrer">
                                    <Button variant="link" size="sm">
                                        {sourceConfig.docsLabel ?? 'Setup guide'} ↗
                                    </Button>
                                </a>
                            )}
                        </CardHeaderRow>
                        <FormField>
                            <Input
                                label={overrides.sharedApiKey.label ?? 'Shared API Key'}
                                placeholder={
                                    isEditing ? 'Enter new API key to replace existing' : 'Enter the shared API key'
                                }
                                value={formState.sharedApiKey}
                                setValue={(val) => updateField('sharedApiKey', val)}
                                isPassword
                                isRequired={!isEditing}
                                error={errors.sharedApiKey}
                                helperText={
                                    overrides.sharedApiKey.helperText ??
                                    'This key will be used for all users. Will be securely encrypted.'
                                }
                            />
                        </FormField>
                        {structuredHeaders && (
                            <StructuredHeadersSection
                                config={structuredHeaders}
                                currentAuthType={formState.authType}
                                headerValues={formState.structuredHeaderValues}
                                onHeaderValueChange={handleHeaderValueChange}
                            />
                        )}
                    </InlineOAuthForm>
                </FormSection>
            )}

            {/* ---- User API Key info (no admin fields, just docs link) ---- */}
            {showAuthSelector && formState.authType === AiPluginAuthType.UserApiKey && (
                <FormSection>
                    <InlineOAuthForm>
                        <CardHeaderRow>
                            <Text weight="bold" style={{ color: colors.gray[600] }}>
                                {structuredHeaders ? structuredHeaders.sectionTitle : 'API Key'}
                            </Text>
                            {sourceConfig.docsUrl && (
                                <a href={sourceConfig.docsUrl} target="_blank" rel="noopener noreferrer">
                                    <Button variant="link" size="sm">
                                        {sourceConfig.docsLabel ?? 'Setup guide'} ↗
                                    </Button>
                                </a>
                            )}
                        </CardHeaderRow>
                        <Text color="gray" size="sm">
                            Each user will provide their own API key in their personal settings.
                        </Text>
                        {structuredHeaders && (
                            <StructuredHeadersSection
                                config={structuredHeaders}
                                currentAuthType={formState.authType}
                                headerValues={formState.structuredHeaderValues}
                                onHeaderValueChange={handleHeaderValueChange}
                            />
                        )}
                    </InlineOAuthForm>
                </FormSection>
            )}

            {/* ---- Custom Headers (main form) ---- */}
            {isVisible('customHeaders') && (
                <FormSection>
                    <CustomHeadersEditor
                        headers={formState.customHeaders}
                        onAdd={addHeader}
                        onUpdate={updateHeader}
                        onRemove={removeHeader}
                    />
                    <Text color="gray" size="sm" style={{ marginTop: 4, display: 'block' }}>
                        {overrides.customHeaders.helperText ?? 'Additional headers to send with every request.'}
                    </Text>
                </FormSection>
            )}

            {/* ---- OAuth Provider (non-custom sources that use OAuth, or Custom with UserOauth) ---- */}
            {showOAuth && (
                <FormSection>
                    {/* Callback URL */}
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
                        <CardHeaderRow>
                            <Text weight="bold" style={{ color: colors.gray[600] }}>
                                Credential Provider
                            </Text>
                            {sourceConfig.docsUrl && (
                                <a href={sourceConfig.docsUrl} target="_blank" rel="noopener noreferrer">
                                    <Button variant="link" size="sm">
                                        {sourceConfig.docsLabel ?? 'Setup guide'} ↗
                                    </Button>
                                </a>
                            )}
                        </CardHeaderRow>

                        {isVisible('oauthServerName') && (
                            <FormField>
                                <Input
                                    label="Provider Name"
                                    placeholder={overrides.oauthServerName.placeholder ?? 'OAuth Provider'}
                                    value={formState.oauthServerName}
                                    setValue={(val) => updateField('oauthServerName', val)}
                                    isRequired
                                    error={errors.oauthServerName}
                                />
                            </FormField>
                        )}

                        {isVisible('oauthServerDescription') && (
                            <FormField>
                                <Input
                                    label="Description"
                                    placeholder="Description of the provider"
                                    value={formState.oauthServerDescription}
                                    setValue={(val) => updateField('oauthServerDescription', val)}
                                />
                            </FormField>
                        )}

                        {isVisible('oauthClientId') && (
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
                        )}

                        {isVisible('oauthClientSecret') && (
                            <FormField>
                                {isEditing && formState.hasOAuthClientSecret && (
                                    <SecretStatus>
                                        <CheckCircle size={14} weight="fill" /> Client secret is configured. Enter new
                                        value to replace.
                                    </SecretStatus>
                                )}
                                <Input
                                    label="Client Secret"
                                    placeholder={
                                        isEditing && formState.hasOAuthClientSecret
                                            ? 'Enter new secret to replace existing'
                                            : 'OAuth Client Secret'
                                    }
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
                        )}

                        {isVisible('oauthAuthorizationUrl') && (
                            <FormField>
                                <Input
                                    label="Authorization URL"
                                    placeholder={
                                        overrides.oauthAuthorizationUrl.placeholder ??
                                        'https://provider.com/oauth/authorize'
                                    }
                                    value={formState.oauthAuthorizationUrl}
                                    setValue={(val) => updateField('oauthAuthorizationUrl', val)}
                                    isRequired
                                    error={errors.oauthAuthorizationUrl}
                                    helperText={overrides.oauthAuthorizationUrl.helperText}
                                />
                            </FormField>
                        )}

                        {isVisible('oauthTokenUrl') && (
                            <FormField>
                                <Input
                                    label="Token URL"
                                    placeholder={
                                        overrides.oauthTokenUrl.placeholder ?? 'https://provider.com/oauth/token'
                                    }
                                    value={formState.oauthTokenUrl}
                                    setValue={(val) => updateField('oauthTokenUrl', val)}
                                    isRequired
                                    error={errors.oauthTokenUrl}
                                    helperText={overrides.oauthTokenUrl.helperText}
                                />
                            </FormField>
                        )}

                        {isVisible('oauthScopes') && (
                            <FormField>
                                <Input
                                    label="Default Scopes"
                                    placeholder={overrides.oauthScopes.placeholder ?? 'openid, profile, repo, user'}
                                    value={formState.oauthScopes}
                                    setValue={(val) => updateField('oauthScopes', val)}
                                    helperText={
                                        overrides.oauthScopes.helperText ??
                                        'Comma-separated scopes requested from the OAuth provider.'
                                    }
                                />
                            </FormField>
                        )}

                        {/* OAuth Advanced Settings (Custom source) */}
                        {isCustom && (
                            <>
                                <AdvancedToggle
                                    onClick={() => setShowOAuthAdvanced(!showOAuthAdvanced)}
                                    style={{ marginTop: 16, marginBottom: 0 }}
                                >
                                    {showOAuthAdvanced ? <CaretDown size={16} /> : <CaretRight size={16} />}
                                    <Text weight="semiBold">Advanced OAuth Settings</Text>
                                </AdvancedToggle>

                                {showOAuthAdvanced && (
                                    <OAuthAdvancedFields formState={formState} updateField={updateField} />
                                )}
                            </>
                        )}
                    </InlineOAuthForm>
                </FormSection>
            )}

            {/* ---- Instructions (main form if visible, advanced otherwise) ---- */}
            {isVisible('instructions') && (
                <FormSection>
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
            )}

            {/* ---- Advanced Settings ---- */}
            {hasAdvanced && (
                <FormSection>
                    <AdvancedToggle onClick={() => setShowAdvanced(!showAdvanced)}>
                        {showAdvanced ? <CaretDown size={16} /> : <CaretRight size={16} />}
                        <Text weight="semiBold">Advanced Settings</Text>
                    </AdvancedToggle>

                    {showAdvanced && (
                        <>
                            {isAdvanced('instructions') && (
                                <FormField>
                                    <TextArea
                                        label="Instructions for the AI Assistant"
                                        placeholder="Use this plugin to search company documentation and internal wikis..."
                                        value={formState.instructions}
                                        onChange={(e) => updateField('instructions', e.target.value)}
                                        rows={4}
                                    />
                                </FormField>
                            )}

                            {isAdvanced('timeout') && (
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
                            )}

                            {isAdvanced('sharedApiKeyAuthScheme') &&
                                formState.authType === AiPluginAuthType.SharedApiKey && (
                                    <FormField>
                                        <Input
                                            label="Auth Scheme"
                                            placeholder="Bearer"
                                            value={formState.sharedApiKeyAuthScheme}
                                            setValue={(val) => updateField('sharedApiKeyAuthScheme', val)}
                                            helperText="Prefix for Authorization header (Bearer, Token, ApiKey)."
                                        />
                                    </FormField>
                                )}

                            {isAdvanced('userApiKeyAuthScheme') &&
                                formState.authType === AiPluginAuthType.UserApiKey && (
                                    <FormField>
                                        <Input
                                            label="Auth Scheme"
                                            placeholder="Bearer"
                                            value={formState.userApiKeyAuthScheme}
                                            setValue={(val) => updateField('userApiKeyAuthScheme', val)}
                                            helperText="Prefix for Authorization header (Bearer, Token, ApiKey)."
                                        />
                                    </FormField>
                                )}

                            {isAdvanced('customHeaders') && (
                                <FormField>
                                    <Text
                                        weight="semiBold"
                                        style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}
                                    >
                                        Custom Headers
                                    </Text>
                                    <CustomHeadersEditor
                                        headers={formState.customHeaders}
                                        onAdd={addHeader}
                                        onUpdate={updateHeader}
                                        onRemove={removeHeader}
                                    />
                                    <Text color="gray" size="sm" style={{ marginTop: 4, display: 'block' }}>
                                        {overrides.customHeaders.helperText ??
                                            'Additional headers to send with every request.'}
                                    </Text>
                                </FormField>
                            )}

                            {/* OAuth advanced fields for Custom source (in the general advanced section) */}
                            {isAdvanced('oauthTokenAuthMethod') &&
                                formState.authType === AiPluginAuthType.UserOauth && (
                                    <OAuthAdvancedFields formState={formState} updateField={updateField} />
                                )}
                        </>
                    )}
                </FormSection>
            )}
        </div>
    );
};
