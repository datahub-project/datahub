/**
 * Pure utility functions for plugin source configuration.
 *
 * Extracted from PluginConfigurationStep so they can be unit-tested
 * independently of the React component.
 */
import { FieldOverride, PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import { PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';

import { AiPluginAuthType } from '@types';

// ---------------------------------------------------------------------------
// Auth-type field groups
// ---------------------------------------------------------------------------

const SHARED_API_KEY_FIELDS: (keyof PluginFormState)[] = ['sharedApiKey', 'sharedApiKeyAuthScheme'];

const USER_API_KEY_FIELDS: (keyof PluginFormState)[] = ['userApiKeyAuthScheme'];

const OAUTH_FIELDS: (keyof PluginFormState)[] = [
    'oauthServerName',
    'oauthServerDescription',
    'oauthClientId',
    'oauthClientSecret',
    'oauthAuthorizationUrl',
    'oauthTokenUrl',
    'oauthScopes',
    'requiredScopes',
    'oauthTokenAuthMethod',
    'oauthAuthLocation',
    'oauthAuthHeaderName',
    'oauthAuthScheme',
    'oauthAuthQueryParam',
];

// ---------------------------------------------------------------------------
// Field visibility
// ---------------------------------------------------------------------------

/**
 * Returns true when `field` is in the provided field list.
 * For sources with multiple allowed auth types, auth-specific fields
 * are conditionally shown based on the current authType selection.
 */
export function shouldShowField(
    field: keyof PluginFormState,
    fieldList: readonly (keyof PluginFormState)[],
    sourceConfig: PluginSourceConfig,
    authType: AiPluginAuthType,
): boolean {
    if (!fieldList.includes(field)) return false;

    // When a source supports multiple auth types, conditionally filter
    // auth-specific fields based on the current selection.
    if (sourceConfig.allowedAuthTypes.length > 1) {
        if (SHARED_API_KEY_FIELDS.includes(field) && authType !== AiPluginAuthType.SharedApiKey) return false;
        if (USER_API_KEY_FIELDS.includes(field) && authType !== AiPluginAuthType.UserApiKey) return false;
        if (OAUTH_FIELDS.includes(field) && authType !== AiPluginAuthType.UserOauth) return false;
    }

    return true;
}

// ---------------------------------------------------------------------------
// Field override resolution
// ---------------------------------------------------------------------------

/**
 * Resolve a field override for a given source config, returning an empty
 * object when no override is defined.
 */
export function getOverride(sourceConfig: PluginSourceConfig, field: keyof PluginFormState): FieldOverride {
    return sourceConfig.fieldOverrides?.[field] ?? {};
}
