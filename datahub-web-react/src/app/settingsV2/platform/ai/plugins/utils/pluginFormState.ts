import { AiPluginAuthType, AiPluginConfig, McpTransport } from '@types';

// ---------------------------------------------------------------------------
// Types for GraphQL query data shapes used when hydrating the form
// ---------------------------------------------------------------------------

/** Shape of the service GraphQL query data used when hydrating form state */
type ServiceQueryData = {
    service?: {
        properties?: {
            displayName?: string | null;
            description?: string | null;
        } | null;
        mcpServerProperties?: {
            url?: string | null;
            transport?: McpTransport | null;
            timeout?: number | null;
            customHeaders?: Array<{ key?: string | null; value?: string | null }> | null;
        } | null;
    } | null;
};

/** Shape of the OAuth server GraphQL query data used when hydrating form state */
type OAuthServerQueryData = {
    oauthAuthorizationServer?: {
        properties?: {
            displayName?: string | null;
            description?: string | null;
            clientId?: string | null;
            authorizationUrl?: string | null;
            tokenUrl?: string | null;
            scopes?: string[] | null;
            tokenAuthMethod?: string | null;
            authLocation?: string | null;
            authHeaderName?: string | null;
            authScheme?: string | null;
            authQueryParam?: string | null;
            hasClientSecret?: boolean | null;
        } | null;
    } | null;
};

/**
 * Represents a custom header key-value pair
 */
export interface CustomHeader {
    id: string;
    key: string;
    value: string;
}

/**
 * Single state object for the plugin form.
 * Used for both create and edit modes.
 */
export interface PluginFormState {
    // Basic info
    displayName: string;
    description: string;

    // Connection settings
    url: string;
    transport: McpTransport;
    timeout: string;

    // Authentication
    authType: AiPluginAuthType;
    sharedApiKey: string;
    sharedApiKeyAuthScheme: string;
    userApiKeyAuthScheme: string;

    // OAuth configuration (for UserOauth auth type)
    oauthServerName: string;
    oauthServerDescription: string;
    oauthClientId: string;
    oauthClientSecret: string;
    oauthAuthorizationUrl: string;
    oauthTokenUrl: string;
    oauthScopes: string;
    requiredScopes: string;

    // OAuth Advanced Settings
    oauthTokenAuthMethod: string; // POST_BODY or BASIC_AUTH
    oauthAuthLocation: string; // HEADER or QUERY_PARAM
    oauthAuthHeaderName: string;
    oauthAuthScheme: string;
    oauthAuthQueryParam: string;

    // OAuth secret status (for showing "configured" indicator when editing)
    hasOAuthClientSecret: boolean;

    // AI Instructions
    instructions: string;

    // Status
    enabled: boolean;

    // Advanced settings
    customHeaders: CustomHeader[];

    // Structured headers (source-specific, e.g. dbt environment config)
    // Maps header keys to their values (e.g. "x-dbt-prod-environment-id" → "123456")
    structuredHeaderValues: Record<string, string>;
}

/**
 * Default initial state for creating a new plugin
 */
export const DEFAULT_PLUGIN_FORM_STATE: PluginFormState = {
    displayName: '',
    description: '',
    url: '',
    transport: McpTransport.Http,
    timeout: '30',
    authType: AiPluginAuthType.None,
    sharedApiKey: '',
    sharedApiKeyAuthScheme: 'Bearer',
    userApiKeyAuthScheme: 'Bearer',
    oauthServerName: '',
    oauthServerDescription: '',
    oauthClientId: '',
    oauthClientSecret: '',
    oauthAuthorizationUrl: '',
    oauthTokenUrl: '',
    oauthScopes: '',
    requiredScopes: '',
    oauthTokenAuthMethod: 'BASIC',
    oauthAuthLocation: 'HEADER',
    oauthAuthHeaderName: 'Authorization',
    oauthAuthScheme: 'Bearer',
    oauthAuthQueryParam: '',
    hasOAuthClientSecret: false,
    instructions: '',
    enabled: true,
    customHeaders: [],
    structuredHeaderValues: {},
};

/**
 * Creates initial form state from an existing plugin (for edit/duplicate modes).
 *
 * @param plugin - The plugin being edited/duplicated
 * @param serviceData - Service query result (used as fallback when plugin lacks mcpServerProperties)
 * @param oauthServerData - OAuth server query result (used to hydrate OAuth fields)
 */
export function createFormStateFromPlugin(
    plugin: AiPluginConfig | null,
    serviceData?: ServiceQueryData,
    oauthServerData?: OAuthServerQueryData,
): PluginFormState {
    if (!plugin) {
        return { ...DEFAULT_PLUGIN_FORM_STATE };
    }

    const service = plugin.service || serviceData?.service;
    const rawHeaders = service?.mcpServerProperties?.customHeaders || [];
    // Normalize nullable values from GraphQL StringMapEntry to safe defaults
    const headers = rawHeaders.map((h) => ({ key: h.key || '', value: h.value || '' }));

    // Build a lookup map from existing headers for structured header hydration
    const headerMap = new Map<string, string>();
    headers.forEach((h) => headerMap.set(h.key, h.value));

    return {
        displayName: service?.properties?.displayName || '',
        description: service?.properties?.description || '',
        url: service?.mcpServerProperties?.url || '',
        transport: service?.mcpServerProperties?.transport || McpTransport.Http,
        timeout: String(service?.mcpServerProperties?.timeout || 30),
        authType: plugin.authType || AiPluginAuthType.None,
        sharedApiKey: '',
        sharedApiKeyAuthScheme: plugin.sharedApiKeyConfig?.authScheme || 'Bearer',
        userApiKeyAuthScheme: plugin.userApiKeyConfig?.authScheme || 'Bearer',
        oauthServerName: oauthServerData?.oauthAuthorizationServer?.properties?.displayName || '',
        oauthServerDescription: oauthServerData?.oauthAuthorizationServer?.properties?.description || '',
        oauthClientId: oauthServerData?.oauthAuthorizationServer?.properties?.clientId || '',
        oauthClientSecret: '', // Never returned from API for security
        oauthAuthorizationUrl: oauthServerData?.oauthAuthorizationServer?.properties?.authorizationUrl || '',
        oauthTokenUrl: oauthServerData?.oauthAuthorizationServer?.properties?.tokenUrl || '',
        oauthScopes: oauthServerData?.oauthAuthorizationServer?.properties?.scopes?.join(', ') || '',
        requiredScopes: plugin.oauthConfig?.requiredScopes?.join(', ') || '',
        oauthTokenAuthMethod: oauthServerData?.oauthAuthorizationServer?.properties?.tokenAuthMethod || 'BASIC',
        oauthAuthLocation: oauthServerData?.oauthAuthorizationServer?.properties?.authLocation || 'HEADER',
        oauthAuthHeaderName: oauthServerData?.oauthAuthorizationServer?.properties?.authHeaderName || 'Authorization',
        oauthAuthScheme: oauthServerData?.oauthAuthorizationServer?.properties?.authScheme || 'Bearer',
        oauthAuthQueryParam: oauthServerData?.oauthAuthorizationServer?.properties?.authQueryParam || '',
        hasOAuthClientSecret: oauthServerData?.oauthAuthorizationServer?.properties?.hasClientSecret ?? false,
        instructions: plugin.instructions || '',
        enabled: plugin.enabled ?? true,
        customHeaders: headers.map((h, i) => ({
            id: `header-${Date.now()}-${i}`,
            key: h.key,
            value: h.value,
        })),
        structuredHeaderValues: Object.fromEntries(headerMap),
    };
}

/**
 * Updates a single field in the form state immutably
 */
export function updateFormField<K extends keyof PluginFormState>(
    state: PluginFormState,
    field: K,
    value: PluginFormState[K],
): PluginFormState {
    return {
        ...state,
        [field]: value,
    };
}

/**
 * Adds a new custom header to the form state
 */
export function addCustomHeader(state: PluginFormState): PluginFormState {
    return {
        ...state,
        customHeaders: [...state.customHeaders, { id: `header-${Date.now()}`, key: '', value: '' }],
    };
}

/**
 * Updates a custom header in the form state
 */
export function updateCustomHeader(
    state: PluginFormState,
    headerId: string,
    field: 'key' | 'value',
    value: string,
): PluginFormState {
    return {
        ...state,
        customHeaders: state.customHeaders.map((header) =>
            header.id === headerId ? { ...header, [field]: value } : header,
        ),
    };
}

/**
 * Removes a custom header from the form state
 */
export function removeCustomHeader(state: PluginFormState, headerId: string): PluginFormState {
    return {
        ...state,
        customHeaders: state.customHeaders.filter((header) => header.id !== headerId),
    };
}

/**
 * Checks if advanced settings should be shown (has custom headers)
 */
export function hasAdvancedSettings(state: PluginFormState): boolean {
    return state.customHeaders.length > 0;
}
