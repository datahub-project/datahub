import { get, omit } from 'lodash';

import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

// Field paths for Snowflake authentication
const authTypeFieldPath = 'source.config.authentication_type';
const passwordFieldPath = 'source.config.password';
const privateKeyFieldPath = 'source.config.private_key';
const privateKeyPasswordFieldPath = 'source.config.private_key_password';

/**
 * Cleans up stale authentication credentials when switching authentication types.
 * This prevents both password and private key from being submitted together.
 *
 * @param recipe - The current recipe configuration
 * @returns Updated recipe with only relevant credentials for the selected auth type
 */
export function setSnowflakeAuthTypeOnRecipe(recipe: any): any {
    let updatedRecipe = { ...recipe };
    const authType = get(updatedRecipe, authTypeFieldPath);

    const passwordFields = [passwordFieldPath];
    const keyPairFields = [privateKeyFieldPath, privateKeyPasswordFieldPath];

    // Remove password when using key pair authentication
    if (authType === 'KEY_PAIR_AUTHENTICATOR') {
        updatedRecipe = omit(updatedRecipe, passwordFields);
    }
    // Remove key pair credentials when using username/password authentication
    else if (authType === 'DEFAULT_AUTHENTICATOR') {
        updatedRecipe = omit(updatedRecipe, keyPairFields);
    }
    // For any other auth type or undefined, clean up all credential fields
    else {
        updatedRecipe = omit(updatedRecipe, [...passwordFields, ...keyPairFields]);
    }

    return updatedRecipe;
}

/**
 * Infers the authentication type from existing recipe credentials.
 * This allows proper form display when editing recipes created via YAML without explicit auth_type.
 *
 * @param recipe - The recipe configuration to inspect
 * @returns The inferred authentication type
 */
export function getSnowflakeAuthTypeFromRecipe(recipe: any): string {
    const hasPassword = !!get(recipe, passwordFieldPath);
    const hasPrivateKey = !!get(recipe, privateKeyFieldPath);

    // If password is present (and no private key), infer DEFAULT_AUTHENTICATOR
    if (hasPassword && !hasPrivateKey) {
        return 'DEFAULT_AUTHENTICATOR';
    }
    // Otherwise default to KEY_PAIR_AUTHENTICATOR (even if private key is not set yet)
    return 'KEY_PAIR_AUTHENTICATOR';
}

/**
 * Helper function to determine if a Snowflake field should be visible based on authentication type.
 * Used for conditional field rendering in forms. Defaults to KEY_PAIR_AUTHENTICATOR if not specified.
 */
export function shouldShowSnowflakeField(fieldName: string, authenticationType?: string): boolean {
    const authType = authenticationType || 'KEY_PAIR_AUTHENTICATOR';

    // Hide password when using key pair authentication
    if (fieldName === 'password' && authType === 'KEY_PAIR_AUTHENTICATOR') {
        return false;
    }
    // Hide private key fields when using default (username/password) authentication
    if ((fieldName === 'private_key' || fieldName === 'private_key_password') && authType === 'DEFAULT_AUTHENTICATOR') {
        return false;
    }
    return true;
}

/**
 * Creates a validator that requires a field value when a specific authentication type is selected.
 * This eliminates duplication across multiple field validators that check authentication type.
 *
 * @param requiredAuthType - The authentication type that requires this field
 * @param fieldLabel - The human-readable label for the field (e.g., 'Password', 'Private Key')
 * @param authTypeLabel - The human-readable label for the auth type (e.g., 'Username & Password', 'Private Key')
 * @returns A validator rule that can be used in RecipeField.rules
 */
export function createAuthTypeValidator(requiredAuthType: string, fieldLabel: string, authTypeLabel: string) {
    return ({ getFieldValue }) => ({
        validator(_, value) {
            const authType = getFieldValue('authentication_type') || 'KEY_PAIR_AUTHENTICATOR';
            if (authType === requiredAuthType && !value) {
                return Promise.reject(new Error(`${fieldLabel} is required for ${authTypeLabel} authentication`));
            }
            return Promise.resolve();
        },
    });
}

export const SNOWFLAKE_ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip:
        'The Snowflake Account Identifier e.g. myorg-account123, account123-eu-central-1, account123.west-us-2.azure',
    type: FieldType.TEXT,
    fieldPath: 'source.config.account_id',
    placeholder: 'xyz123',
    rules: null,
    required: true,
};

export const SNOWFLAKE_WAREHOUSE: RecipeField = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'The name of the Snowflake Warehouse to extract metadata from.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse',
    placeholder: 'COMPUTE_WH',
    rules: null,
    required: true,
};

export const SNOWFLAKE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'snowflake',
    rules: null,
    required: true,
};

export const SNOWFLAKE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
    required: true,
};

export const SNOWFLAKE_ROLE: RecipeField = {
    name: 'role',
    label: 'Role',
    tooltip: 'The Role to use when extracting metadata from Snowflake.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.role',
    placeholder: 'datahub_role',
    rules: null,
    required: true,
};

export const SNOWFLAKE_AUTHENTICATION_TYPE: RecipeField = {
    name: 'authentication_type',
    label: 'Authentication Type',
    tooltip: 'Choose the authentication method for connecting to Snowflake.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.authentication_type',
    options: [
        { label: 'Private Key', value: 'KEY_PAIR_AUTHENTICATOR' },
        { label: 'Username & Password', value: 'DEFAULT_AUTHENTICATOR' },
    ],
    placeholder: 'KEY_PAIR_AUTHENTICATOR',
    rules: null,
    required: true,
    setValueOnRecipeOverride: setSnowflakeAuthTypeOnRecipe,
    getValueFromRecipeOverride: getSnowflakeAuthTypeFromRecipe,
};

export const SNOWFLAKE_PRIVATE_KEY: RecipeField = {
    name: 'private_key',
    label: 'Private Key',
    tooltip:
        'RSA private key for key pair authentication. Can be provided as a string or path to key file. Required when using Private Key authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key',
    placeholder: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----',
    rules: [createAuthTypeValidator('KEY_PAIR_AUTHENTICATOR', 'Private Key', 'Private Key')],
    required: false,
    shouldShow: (formValues) => shouldShowSnowflakeField('private_key', formValues.authentication_type),
};

export const SNOWFLAKE_PRIVATE_KEY_PASSWORD: RecipeField = {
    name: 'private_key_password',
    label: 'Private Key Password',
    tooltip: 'Password for the private key if it is encrypted. Leave blank if your private key is not encrypted.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key_password',
    placeholder: 'Leave blank if key is not encrypted',
    rules: null,
    required: false,
    shouldShow: (formValues) => shouldShowSnowflakeField('private_key_password', formValues.authentication_type),
};
