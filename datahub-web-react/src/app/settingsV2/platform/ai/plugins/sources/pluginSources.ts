/**
 * Plugin Source Registry
 *
 * To add a new MCP plugin source:
 * 1. Add a new PluginSourceConfig entry to PLUGIN_SOURCES below.
 * 2. Add the logo import to pluginLogoUtils.ts (if not already present).
 * 3. That's it — the UI renders automatically based on this config.
 *
 * No UI component changes are needed for standard sources.
 */
import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';

import { AiPluginAuthType, McpTransport } from '@types';

// ---------------------------------------------------------------------------
// Source definitions (alphabetical, "Custom" always last)
// ---------------------------------------------------------------------------

const dbtSource: PluginSourceConfig = {
    name: 'dbt',
    displayName: 'dbt Cloud MCP',
    description: 'Check job runs, model definitions, and test results.',
    configSubtitle: 'Connect your dbt Cloud account to Ask DataHub',
    datahubDocsUrl: 'https://docs.datahub.com/docs/features/feature-guides/ask-datahub/plugins/dbt',
    allowedAuthTypes: [AiPluginAuthType.SharedApiKey, AiPluginAuthType.UserApiKey],
    docsUrl: 'https://docs.getdbt.com/docs/dbt-ai/setup-remote-mcp',
    docsLabel: 'Find your access token',
    defaults: {
        authType: AiPluginAuthType.SharedApiKey,
        sharedApiKeyAuthScheme: 'Token',
        userApiKeyAuthScheme: 'Token',
        transport: McpTransport.Http,
    },
    visibleFields: ['displayName', 'description', 'url', 'sharedApiKey', 'instructions'],
    advancedFields: ['timeout', 'customHeaders'],
    fieldOverrides: {
        url: {
            label: 'MCP Server URL',
            placeholder: 'https://cloud.getdbt.com/api/ai/v1/mcp/',
            helperText: 'Some deployments use ACCOUNT_PREFIX.us1.dbt.com format.',
        },
        sharedApiKey: {
            label: 'Access Token',
            helperText: 'Service token or personal access token with Semantic Layer permissions.',
        },
    },
    structuredHeaders: {
        sectionTitle: 'Configuration',
        fields: [
            {
                headerKey: 'x-dbt-prod-environment-id',
                label: 'Production Environment ID',
                placeholder: 'e.g. 123456',
                helperText: 'Found on the Orchestration page in dbt Cloud.',
                required: true,
            },
            {
                headerKey: 'x-dbt-dev-environment-id',
                label: 'Development Environment ID',
                placeholder: 'e.g. 789012',
                helperText: 'Required for SQL execution.',
            },
            {
                headerKey: 'x-dbt-user-id',
                label: 'User ID',
                placeholder: 'e.g. 456789',
                helperText: 'Required for SQL execution.',
                visibleForAuthTypes: [AiPluginAuthType.SharedApiKey],
            },
        ],
    },
};

const githubSource: PluginSourceConfig = {
    name: 'github',
    displayName: 'GitHub MCP',
    description: 'Browse repositories, raise PRs, and manage issues.',
    configSubtitle: 'Connect your GitHub OAuth App to Ask DataHub',
    datahubDocsUrl: 'https://docs.datahub.com/docs/features/feature-guides/ask-datahub/plugins/github',
    allowedAuthTypes: [AiPluginAuthType.UserOauth],
    docsUrl: 'https://github.com/settings/developers',
    docsLabel: 'Create your OAuth app',
    defaults: {
        url: 'https://api.githubcopilot.com/mcp/',
        authType: AiPluginAuthType.UserOauth,
        oauthAuthorizationUrl: 'https://github.com/login/oauth/authorize',
        oauthTokenUrl: 'https://github.com/login/oauth/access_token',
        oauthServerName: 'GitHub OAuth',
        oauthScopes: 'repo',
        transport: McpTransport.Http,
        // GitHub uses POST body for token exchange
        oauthTokenAuthMethod: 'POST_BODY',
    },
    visibleFields: ['displayName', 'description', 'oauthClientId', 'oauthClientSecret', 'oauthScopes', 'instructions'],
    advancedFields: ['timeout', 'customHeaders'],
    fieldOverrides: {
        oauthScopes: {
            placeholder: 'repo, read:org, user',
            helperText: 'Comma-separated GitHub OAuth scopes.',
        },
    },
};

const snowflakeSource: PluginSourceConfig = {
    name: 'snowflake',
    displayName: 'Snowflake MCP',
    description: 'Query and explore your Snowflake data.',
    configSubtitle: 'Connect your Snowflake account to Ask DataHub',
    datahubDocsUrl: 'https://docs.datahub.com/docs/features/feature-guides/ask-datahub/plugins/snowflake',
    allowedAuthTypes: [AiPluginAuthType.UserOauth],
    docsUrl: 'https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp#set-up-oauth-authentication',
    docsLabel: 'OAuth setup guide',
    defaults: {
        authType: AiPluginAuthType.UserOauth,
        oauthScopes: 'refresh_token, session:role:<ROLE_NAME>',
        transport: McpTransport.Http,
        // Snowflake uses POST body for token exchange
        oauthTokenAuthMethod: 'POST_BODY',
        oauthServerName: 'Snowflake OAuth',
    },
    visibleFields: [
        'displayName',
        'description',
        'url',
        'oauthClientId',
        'oauthClientSecret',
        'oauthAuthorizationUrl',
        'oauthTokenUrl',
        'oauthScopes',
        'instructions',
    ],
    advancedFields: ['timeout', 'customHeaders'],
    fieldOverrides: {
        url: {
            label: 'MCP Server URL',
            placeholder:
                'https://<account>.snowflakecomputing.com/api/v2/databases/<DB>/schemas/<SCHEMA>/mcp-servers/<NAME>',
            helperText: 'You must first create an MCP server in Snowflake.',
            helperDocsUrl: 'https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp',
            helperDocsLabel: 'Snowflake MCP setup guide',
        },
        oauthScopes: {
            placeholder: 'refresh_token, session:role:DATA_ANALYST',
            helperText: 'Replace <ROLE_NAME> with the Snowflake role to use (e.g. DATA_ANALYST).',
        },
        oauthAuthorizationUrl: {
            placeholder: 'https://<ACCOUNT>.snowflakecomputing.com/oauth/authorize',
            helperText: 'Found in your Snowflake OAuth configuration.',
        },
        oauthTokenUrl: {
            placeholder: 'https://<ACCOUNT>.snowflakecomputing.com/oauth/token-request',
            helperText: 'Found in your Snowflake OAuth configuration.',
        },
    },
};

const customSource: PluginSourceConfig = {
    name: 'custom',
    displayName: 'Custom MCP',
    description: 'Add custom data & tools via a remote server.',
    configSubtitle: 'Configure a remote MCP server for Ask DataHub',
    allowedAuthTypes: [
        AiPluginAuthType.None,
        AiPluginAuthType.SharedApiKey,
        AiPluginAuthType.UserApiKey,
        AiPluginAuthType.UserOauth,
    ],
    defaults: {},
    visibleFields: [
        'displayName',
        'description',
        'url',
        'authType',
        // Shared API Key fields (conditionally shown based on authType)
        'sharedApiKey',
        // OAuth fields (conditionally shown based on authType)
        'oauthServerName',
        'oauthServerDescription',
        'oauthClientId',
        'oauthClientSecret',
        'oauthAuthorizationUrl',
        'oauthTokenUrl',
        'oauthScopes',
        'instructions',
    ],
    advancedFields: [
        'timeout',
        'customHeaders',
        'sharedApiKeyAuthScheme',
        'userApiKeyAuthScheme',
        'oauthTokenAuthMethod',
        'oauthAuthLocation',
        'oauthAuthHeaderName',
        'oauthAuthScheme',
        'oauthAuthQueryParam',
    ],
};

// ---------------------------------------------------------------------------
// Registry — sources appear in this order in the card grid
// ---------------------------------------------------------------------------

export const PLUGIN_SOURCES: PluginSourceConfig[] = [customSource, dbtSource, githubSource, snowflakeSource];

/**
 * Look up a source config by its name key.
 * Falls back to `custom` when no match is found.
 */
export function getPluginSource(name: string): PluginSourceConfig {
    return PLUGIN_SOURCES.find((s) => s.name === name) ?? customSource;
}

/**
 * Attempt to detect which source config matches an existing plugin.
 * Uses the URL or display name to infer the source.
 * Returns 'custom' when no match is found.
 */
export function detectPluginSourceName(url?: string | null, displayName?: string | null): string {
    if (!url && !displayName) return 'custom';

    const lowerUrl = url?.toLowerCase() ?? '';
    const lowerName = displayName?.toLowerCase() ?? '';

    if (lowerUrl.includes('githubcopilot.com') || lowerUrl.includes('github.com')) return 'github';
    if (lowerUrl.includes('getdbt.com') || lowerUrl.includes('dbt.com')) return 'dbt';
    if (lowerUrl.includes('snowflake') || lowerUrl.includes('snowflakecomputing.com')) return 'snowflake';

    // Fallback: check display name keywords
    if (lowerName.includes('github')) return 'github';
    if (lowerName.includes('dbt')) return 'dbt';
    if (lowerName.includes('snowflake')) return 'snowflake';

    return 'custom';
}
