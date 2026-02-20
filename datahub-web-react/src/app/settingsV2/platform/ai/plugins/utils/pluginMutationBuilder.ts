import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import { PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';

import { AiPluginAuthType, AuthLocation, McpTransport, ServiceSubType, TokenAuthMethod } from '@types';

/**
 * OAuth server input for mutation
 */
export interface OAuthServerInput {
    id?: string;
    displayName: string;
    description?: string;
    clientId: string;
    // Optional to allow preserving existing secret when editing
    // Backend interprets: undefined = preserve existing, value = replace
    clientSecret?: string;
    authorizationUrl: string;
    tokenUrl: string;
    scopes?: string[];
    // Advanced OAuth settings
    tokenAuthMethod?: TokenAuthMethod;
    authLocation?: AuthLocation;
    authHeaderName?: string;
    authScheme?: string;
    authQueryParam?: string;
}

/**
 * Custom header input for mutation
 */
export interface CustomHeaderInput {
    key: string;
    value: string;
}

/**
 * MCP server properties input for mutation
 */
export interface McpServerPropertiesInput {
    url: string;
    transport: McpTransport;
    timeout: number;
    customHeaders?: CustomHeaderInput[];
}

/**
 * Service upsert mutation input
 */
export interface UpsertAiPluginInput {
    id?: string;
    displayName: string;
    description?: string;
    subType: ServiceSubType;
    mcpServerProperties: McpServerPropertiesInput;
    enabled: boolean;
    instructions?: string;
    authType: AiPluginAuthType;
    oauthServerUrn?: string; // Link to existing OAuth server (for edits)
    newOAuthServer?: OAuthServerInput; // Create new OAuth server inline (for creates)
    sharedApiKey?: string;
    sharedApiKeyAuthScheme?: string;
    userApiKeyAuthScheme?: string;
    requiredScopes?: string[];
}

/**
 * Options for building mutation variables
 */
export interface MutationBuilderOptions {
    editingUrn?: string | null;
    existingOAuthServerUrn?: string | null; // URN of existing OAuth server (for edits - link via oauthServerUrn)
    sourceConfig?: PluginSourceConfig; // Source config for structured header merging
}

/**
 * Parses a comma-separated string into an array of trimmed strings
 */
export function parseCommaSeparatedList(value: string): string[] | undefined {
    if (!value.trim()) {
        return undefined;
    }
    return value
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
}

/**
 * Builds structured header entries from form state and source config.
 * Converts the structuredHeaderValues into CustomHeaderInput entries that the backend understands.
 * Respects visibleForAuthTypes so fields hidden for the current auth type are excluded.
 */
export function buildStructuredHeadersInput(
    state: PluginFormState,
    sourceConfig?: PluginSourceConfig,
): CustomHeaderInput[] {
    if (!sourceConfig?.structuredHeaders) return [];

    const headers: CustomHeaderInput[] = [];
    const { structuredHeaders } = sourceConfig;

    // Fields (filtered by auth type visibility)
    structuredHeaders.fields.forEach((field) => {
        if (field.visibleForAuthTypes && !field.visibleForAuthTypes.includes(state.authType)) return;
        const value = state.structuredHeaderValues[field.headerKey];
        if (value?.trim()) {
            headers.push({ key: field.headerKey, value: value.trim() });
        }
    });

    return headers;
}

/**
 * Builds custom headers input from form state, merging structured headers when available.
 */
export function buildCustomHeadersInput(
    state: PluginFormState,
    sourceConfig?: PluginSourceConfig,
): CustomHeaderInput[] | undefined {
    // Start with structured headers (source-specific)
    const structuredHeaders = buildStructuredHeadersInput(state, sourceConfig);
    const structuredKeys = new Set(structuredHeaders.map((h) => h.key));

    // Add raw custom headers, excluding any that overlap with structured header keys
    const rawHeaders = state.customHeaders
        .filter((h) => h.key.trim() && !structuredKeys.has(h.key.trim()))
        .map((h) => ({ key: h.key.trim(), value: h.value.trim() }));

    const allHeaders = [...structuredHeaders, ...rawHeaders];

    if (allHeaders.length === 0) {
        return undefined;
    }

    return allHeaders;
}

/**
 * Builds OAuth server input for updating an existing OAuth server.
 * Used when editing a plugin with an existing OAuth configuration.
 */
export function buildUpsertOAuthServerInput(state: PluginFormState, existingOAuthServerId: string): OAuthServerInput {
    // Convert string values to enum types
    const tokenAuthMethod = state.oauthTokenAuthMethod as TokenAuthMethod | undefined;
    const authLocation = state.oauthAuthLocation as AuthLocation | undefined;

    return {
        id: existingOAuthServerId,
        displayName: state.oauthServerName,
        description: state.oauthServerDescription || undefined,
        // Trim credentials and URLs that get sent to external OAuth providers
        clientId: state.oauthClientId.trim(),
        // Send undefined instead of empty string to preserve existing secret
        // Backend interprets: undefined/null = preserve existing, empty string = clear, value = replace
        clientSecret: state.oauthClientSecret.trim() || undefined,
        authorizationUrl: state.oauthAuthorizationUrl.trim(),
        tokenUrl: state.oauthTokenUrl.trim(),
        scopes: parseCommaSeparatedList(state.oauthScopes),
        // Advanced OAuth settings
        tokenAuthMethod: tokenAuthMethod || undefined,
        authLocation: authLocation || undefined,
        authHeaderName: state.oauthAuthHeaderName || undefined,
        authScheme: state.oauthAuthScheme || undefined,
        authQueryParam: state.oauthAuthQueryParam || undefined,
    };
}

/**
 * Builds OAuth server input for creating a new OAuth server inline.
 * Used when creating a new plugin with OAuth.
 */
export function buildNewOAuthServerInput(state: PluginFormState): OAuthServerInput | undefined {
    if (state.authType !== AiPluginAuthType.UserOauth) {
        return undefined;
    }

    // Convert string values to enum types
    const tokenAuthMethod = state.oauthTokenAuthMethod as TokenAuthMethod | undefined;
    const authLocation = state.oauthAuthLocation as AuthLocation | undefined;

    return {
        displayName: state.oauthServerName,
        description: state.oauthServerDescription || undefined,
        // Trim credentials and URLs that get sent to external OAuth providers
        clientId: state.oauthClientId.trim(),
        clientSecret: state.oauthClientSecret.trim(),
        authorizationUrl: state.oauthAuthorizationUrl.trim(),
        tokenUrl: state.oauthTokenUrl.trim(),
        scopes: parseCommaSeparatedList(state.oauthScopes),
        // Advanced OAuth settings
        tokenAuthMethod: tokenAuthMethod || undefined,
        authLocation: authLocation || undefined,
        authHeaderName: state.oauthAuthHeaderName || undefined,
        authScheme: state.oauthAuthScheme || undefined,
        authQueryParam: state.oauthAuthQueryParam || undefined,
    };
}

/**
 * Builds the complete mutation input from form state
 */
export function buildUpsertAiPluginInput(state: PluginFormState, options: MutationBuilderOptions): UpsertAiPluginInput {
    const { editingUrn, existingOAuthServerUrn, sourceConfig } = options;

    // Determine OAuth handling:
    // - existingOAuthServerUrn: editing with existing OAuth server (updated via separate mutation, link via oauthServerUrn)
    // - no existingOAuthServerUrn: creating new OAuth server inline (use newOAuthServer)
    const useExistingOAuthServer = !!existingOAuthServerUrn && state.authType === AiPluginAuthType.UserOauth;
    const createNewOAuthServer = !existingOAuthServerUrn && state.authType === AiPluginAuthType.UserOauth;

    return {
        id: editingUrn ? editingUrn.split(':').pop() : undefined,
        displayName: state.displayName,
        description: state.description || undefined,
        subType: ServiceSubType.McpServer,
        mcpServerProperties: {
            url: state.url.trim(),
            transport: state.transport,
            timeout: parseInt(state.timeout, 10) || 30,
            customHeaders: buildCustomHeadersInput(state, sourceConfig),
        },
        enabled: state.enabled,
        instructions: state.instructions || undefined,
        authType: state.authType,
        // Link to existing OAuth server (for edits - OAuth server updated via separate mutation)
        oauthServerUrn: useExistingOAuthServer ? existingOAuthServerUrn : undefined,
        // Create new OAuth server inline (for creates)
        newOAuthServer: createNewOAuthServer ? buildNewOAuthServerInput(state) : undefined,
        // Trim credentials and auth values that get sent to external APIs
        sharedApiKey:
            state.authType === AiPluginAuthType.SharedApiKey ? state.sharedApiKey.trim() || undefined : undefined,
        sharedApiKeyAuthScheme:
            state.authType === AiPluginAuthType.SharedApiKey && state.sharedApiKeyAuthScheme
                ? state.sharedApiKeyAuthScheme
                : undefined,
        userApiKeyAuthScheme:
            state.authType === AiPluginAuthType.UserApiKey && state.userApiKeyAuthScheme
                ? state.userApiKeyAuthScheme
                : undefined,
        requiredScopes: parseCommaSeparatedList(state.requiredScopes),
    };
}

/**
 * Extracts the OAuth server ID from a URN
 */
export function extractOAuthServerIdFromUrn(urn: string | null | undefined): string | null {
    if (!urn) return null;
    return urn.split(':').pop() || null;
}
