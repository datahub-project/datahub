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
export interface UpsertServiceInput {
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
 * Builds custom headers input from form state
 */
export function buildCustomHeadersInput(state: PluginFormState): CustomHeaderInput[] | undefined {
    const validHeaders = state.customHeaders.filter((h) => h.key.trim());

    if (validHeaders.length === 0) {
        return undefined;
    }

    return validHeaders.map((h) => ({ key: h.key, value: h.value }));
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
        clientId: state.oauthClientId,
        // Send undefined instead of empty string to preserve existing secret
        // Backend interprets: undefined/null = preserve existing, empty string = clear, value = replace
        clientSecret: state.oauthClientSecret || undefined,
        authorizationUrl: state.oauthAuthorizationUrl,
        tokenUrl: state.oauthTokenUrl,
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
        clientId: state.oauthClientId,
        clientSecret: state.oauthClientSecret,
        authorizationUrl: state.oauthAuthorizationUrl,
        tokenUrl: state.oauthTokenUrl,
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
export function buildUpsertServiceInput(state: PluginFormState, options: MutationBuilderOptions): UpsertServiceInput {
    const { editingUrn, existingOAuthServerUrn } = options;

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
            url: state.url,
            transport: state.transport,
            timeout: parseInt(state.timeout, 10) || 30,
            customHeaders: buildCustomHeadersInput(state),
        },
        enabled: state.enabled,
        instructions: state.instructions || undefined,
        authType: state.authType,
        // Link to existing OAuth server (for edits - OAuth server updated via separate mutation)
        oauthServerUrn: useExistingOAuthServer ? existingOAuthServerUrn : undefined,
        // Create new OAuth server inline (for creates)
        newOAuthServer: createNewOAuthServer ? buildNewOAuthServerInput(state) : undefined,
        sharedApiKey: state.authType === AiPluginAuthType.SharedApiKey ? state.sharedApiKey || undefined : undefined,
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
