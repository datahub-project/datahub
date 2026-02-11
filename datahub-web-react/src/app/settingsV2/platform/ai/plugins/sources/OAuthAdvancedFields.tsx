import React from 'react';
import styled from 'styled-components';

import { PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';
import { Input, SimpleSelect, Text, colors } from '@src/alchemy-components';

// ---------------------------------------------------------------------------
// Styled components
// ---------------------------------------------------------------------------

const FormField = styled.div`
    margin-bottom: 16px;
`;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TOKEN_AUTH_METHOD_OPTIONS = [
    { label: 'Basic Auth (default)', value: 'BASIC' },
    { label: 'POST Body', value: 'POST_BODY' },
    { label: 'None (Public Client)', value: 'NONE' },
];

const AUTH_LOCATION_OPTIONS = [
    { label: 'Header (default)', value: 'HEADER' },
    { label: 'Query Parameter', value: 'QUERY_PARAM' },
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Props = {
    formState: PluginFormState;
    updateField: <K extends keyof PluginFormState>(field: K, value: PluginFormState[K]) => void;
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

/**
 * Advanced OAuth settings for the Custom source type.
 * Renders token auth method, auth injection location, and related fields.
 */
export function OAuthAdvancedFields({ formState, updateField }: Props) {
    return (
        <>
            <FormField style={{ marginTop: 12 }}>
                <Text weight="semiBold" style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}>
                    Token Auth Method
                </Text>
                <SimpleSelect
                    options={TOKEN_AUTH_METHOD_OPTIONS}
                    values={[formState.oauthTokenAuthMethod]}
                    onUpdate={(vals) => updateField('oauthTokenAuthMethod', vals[0])}
                    width="full"
                />
                <Text color="gray" size="sm" style={{ marginTop: 4 }}>
                    How to send client credentials when exchanging tokens. Use &quot;None&quot; for public OAuth
                    clients.
                </Text>
            </FormField>

            <FormField>
                <Text weight="semiBold" style={{ marginBottom: 8, display: 'block', color: colors.gray[600] }}>
                    Auth Injection Location
                </Text>
                <SimpleSelect
                    options={AUTH_LOCATION_OPTIONS}
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
    );
}
