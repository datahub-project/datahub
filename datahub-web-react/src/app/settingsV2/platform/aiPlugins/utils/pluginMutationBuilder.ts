import { PluginFormState } from '@app/settingsV2/platform/aiPlugins/utils/pluginFormState';

import { AiPluginAuthType, McpTransport, ServiceSubType } from '@types';

/**
 * OAuth server input for mutation
 */
export interface OAuthServerInput {
    id?: string;
    displayName: string;
    description?: string;
    clientId: string;
    clientSecret: string;
    authorizationUrl: string;
    tokenUrl: string;
    scopes?: string[];
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
    newOAuthServer?: OAuthServerInput;
    sharedApiKey?: string;
    sharedApiKeyAuthScheme?: string;
    requiredScopes?: string[];
}

/**
 * Options for building mutation variables
 */
export interface MutationBuilderOptions {
    editingUrn?: string | null;
    existingOAuthServerId?: string | null;
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
 * Builds OAuth server input from form state
 */
export function buildOAuthServerInput(
    state: PluginFormState,
    existingOAuthServerId?: string | null,
): OAuthServerInput | undefined {
    if (state.authType !== AiPluginAuthType.UserOauth) {
        return undefined;
    }

    return {
        id: existingOAuthServerId || undefined,
        displayName: state.oauthServerName,
        description: state.oauthServerDescription || undefined,
        clientId: state.oauthClientId,
        clientSecret: state.oauthClientSecret,
        authorizationUrl: state.oauthAuthorizationUrl,
        tokenUrl: state.oauthTokenUrl,
        scopes: parseCommaSeparatedList(state.oauthScopes),
    };
}

/**
 * Builds the complete mutation input from form state
 */
export function buildUpsertServiceInput(state: PluginFormState, options: MutationBuilderOptions): UpsertServiceInput {
    const { editingUrn, existingOAuthServerId } = options;

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
        newOAuthServer: buildOAuthServerInput(state, existingOAuthServerId),
        sharedApiKey: state.authType === AiPluginAuthType.SharedApiKey ? state.sharedApiKey || undefined : undefined,
        sharedApiKeyAuthScheme:
            state.authType === AiPluginAuthType.SharedApiKey && state.sharedApiKeyAuthScheme
                ? state.sharedApiKeyAuthScheme
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
