import { PluginFormState } from '@app/settingsV2/platform/aiPlugins/utils/pluginFormState';

import { AiPluginAuthType } from '@types';

/**
 * Validation errors keyed by field name
 */
export type ValidationErrors = Record<string, string>;

/**
 * Result of form validation
 */
export interface ValidationResult {
    isValid: boolean;
    errors: ValidationErrors;
}

/**
 * Options for validation
 */
export interface ValidationOptions {
    isEditing: boolean;
    existingNames: string[];
    originalName?: string | null;
}

/**
 * Validates the display name field
 */
export function validateDisplayName(
    displayName: string,
    existingNames: string[],
    originalName?: string | null,
): string | null {
    const trimmed = displayName.trim();

    if (!trimmed) {
        return 'Name is required';
    }

    // Check for duplicate names (exclude original name when editing)
    const isDuplicate = existingNames.some(
        (name) => name.toLowerCase() === trimmed.toLowerCase() && name !== originalName,
    );

    if (isDuplicate) {
        return 'A plugin with this name already exists';
    }

    return null;
}

/**
 * Validates the server URL field
 */
export function validateUrl(url: string): string | null {
    if (!url.trim()) {
        return 'Server URL is required';
    }
    return null;
}

/**
 * Validates shared API key fields
 */
export function validateSharedApiKey(sharedApiKey: string, isEditing: boolean): string | null {
    // When editing, API key is optional (to keep existing)
    if (!sharedApiKey && !isEditing) {
        return 'Shared API key is required';
    }
    return null;
}

/**
 * Validates OAuth configuration fields
 */
export function validateOAuthConfig(state: PluginFormState): ValidationErrors {
    const errors: ValidationErrors = {};

    if (!state.oauthServerName.trim()) {
        errors.oauthServerName = 'Provider name is required';
    }
    if (!state.oauthClientId.trim()) {
        errors.oauthClientId = 'Client ID is required';
    }
    if (!state.oauthClientSecret.trim()) {
        errors.oauthClientSecret = 'Client Secret is required';
    }
    if (!state.oauthAuthorizationUrl.trim()) {
        errors.oauthAuthorizationUrl = 'Authorization URL is required';
    }
    if (!state.oauthTokenUrl.trim()) {
        errors.oauthTokenUrl = 'Token URL is required';
    }

    return errors;
}

/**
 * Validates the entire plugin form
 */
export function validatePluginForm(state: PluginFormState, options: ValidationOptions): ValidationResult {
    const errors: ValidationErrors = {};

    // Validate basic fields
    const displayNameError = validateDisplayName(state.displayName, options.existingNames, options.originalName);
    if (displayNameError) {
        errors.displayName = displayNameError;
    }

    const urlError = validateUrl(state.url);
    if (urlError) {
        errors.url = urlError;
    }

    // Validate auth-type specific fields
    if (state.authType === AiPluginAuthType.SharedApiKey) {
        const apiKeyError = validateSharedApiKey(state.sharedApiKey, options.isEditing);
        if (apiKeyError) {
            errors.sharedApiKey = apiKeyError;
        }
    }

    if (state.authType === AiPluginAuthType.UserOauth) {
        const oauthErrors = validateOAuthConfig(state);
        Object.assign(errors, oauthErrors);
    }

    return {
        isValid: Object.keys(errors).length === 0,
        errors,
    };
}

/**
 * Checks if the form is valid for submission (quick check without full error messages)
 */
export function isFormValid(state: PluginFormState, options: ValidationOptions): boolean {
    // Basic required fields
    if (!state.displayName.trim() || !state.url.trim()) {
        return false;
    }

    // Check for duplicate names
    const isDuplicate = options.existingNames.some(
        (name) => name.toLowerCase() === state.displayName.trim().toLowerCase() && name !== options.originalName,
    );
    if (isDuplicate) {
        return false;
    }

    // Auth type specific validation
    if (state.authType === AiPluginAuthType.SharedApiKey && !state.sharedApiKey && !options.isEditing) {
        return false;
    }

    if (state.authType === AiPluginAuthType.UserOauth) {
        if (
            !state.oauthServerName.trim() ||
            !state.oauthClientId.trim() ||
            !state.oauthClientSecret.trim() ||
            !state.oauthAuthorizationUrl.trim() ||
            !state.oauthTokenUrl.trim()
        ) {
            return false;
        }
    }

    return true;
}
