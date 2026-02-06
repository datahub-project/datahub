import { PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';

import { AiPluginAuthType } from '@types';

/**
 * Validation errors keyed by field name
 */
export type ValidationErrors = Record<string, string>;

/**
 * Validates that a string is a valid URL with http:// or https:// protocol.
 * Returns an error message if invalid, or null if valid.
 */
export function validateUrlFormat(url: string, fieldLabel = 'URL'): string | null {
    const trimmed = url.trim();

    if (!trimmed) {
        return `${fieldLabel} is required`;
    }

    // Check for valid URL format
    try {
        const parsedUrl = new URL(trimmed);

        // Must be http or https protocol
        if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
            return `${fieldLabel} must start with http:// or https://`;
        }

        // Must have a hostname
        if (!parsedUrl.hostname) {
            return `${fieldLabel} must include a valid hostname`;
        }

        return null;
    } catch {
        return `${fieldLabel} must be a valid URL (e.g., https://example.com)`;
    }
}

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
    return validateUrlFormat(url, 'Server URL');
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
export function validateOAuthConfig(state: PluginFormState, _isEditing: boolean): ValidationErrors {
    const errors: ValidationErrors = {};

    if (!state.oauthServerName.trim()) {
        errors.oauthServerName = 'Provider name is required';
    }
    if (!state.oauthClientId.trim()) {
        errors.oauthClientId = 'Client ID is required';
    }
    // Client Secret is optional - backend will keep existing if not provided

    // Validate Authorization URL format
    const authUrlError = validateUrlFormat(state.oauthAuthorizationUrl, 'Authorization URL');
    if (authUrlError) {
        errors.oauthAuthorizationUrl = authUrlError;
    }

    // Validate Token URL format
    const tokenUrlError = validateUrlFormat(state.oauthTokenUrl, 'Token URL');
    if (tokenUrlError) {
        errors.oauthTokenUrl = tokenUrlError;
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
        const oauthErrors = validateOAuthConfig(state, options.isEditing);
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

    // Validate Server URL format
    if (validateUrlFormat(state.url, 'URL') !== null) {
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
            // Client Secret is optional - backend will keep existing if not provided
            validateUrlFormat(state.oauthAuthorizationUrl, 'URL') !== null ||
            validateUrlFormat(state.oauthTokenUrl, 'URL') !== null
        ) {
            return false;
        }
    }

    return true;
}
