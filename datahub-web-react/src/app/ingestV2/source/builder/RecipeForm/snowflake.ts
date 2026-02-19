import { get, omit } from 'lodash';

import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

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
 * Creates a validator that requires a field value when a specific authentication type is selected.
 * This eliminates duplication across multiple field validators that check authentication type.
 *
 * @param requiredAuthType - The authentication type that requires this field
 * @param fieldLabel - The human-readable label for the field (e.g., 'Password', 'Private Key')
 * @param authTypeLabel - The human-readable label for the auth type (e.g., 'Username & Password', 'Private Key')
 * @returns A validator rule that can be used in RecipeField.rules
 */
function createAuthTypeValidator(requiredAuthType: string, fieldLabel: string, authTypeLabel: string) {
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
    helper: 'Snowflake Account Identifier',
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
    helper: 'Snowflake Warehouse name',
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
    helper: 'Snowflake username for metadata',
    tooltip: 'Snowflake username.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'snowflake',
    rules: null,
    required: true,
};

export const SNOWFLAKE_AUTHENTICATION_TYPE: RecipeField = {
    name: 'authentication_type',
    label: 'Authentication Type',
    helper: 'Choose how to authenticate',
    tooltip: 'Choose the authentication method for connecting to Snowflake.',
    type: FieldType.SELECT,
    fieldPath: authTypeFieldPath,
    options: [
        { label: 'Private Key', value: 'KEY_PAIR_AUTHENTICATOR' },
        { label: 'Username & Password', value: 'DEFAULT_AUTHENTICATOR' },
    ],
    placeholder: 'KEY_PAIR_AUTHENTICATOR',
    rules: [{ required: true, message: 'Authentication Type is required' }],
    required: true,
    setValueOnRecipeOverride: setSnowflakeAuthTypeOnRecipe,
    getValueFromRecipeOverride: getSnowflakeAuthTypeFromRecipe,
};

export const SNOWFLAKE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    helper: 'Snowflake password for user',
    tooltip: 'Snowflake password. Required when using Username & Password authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: [createAuthTypeValidator('DEFAULT_AUTHENTICATOR', 'Password', 'Username & Password')],
    required: false,
    dynamicHidden: (values) => (values?.authentication_type || 'KEY_PAIR_AUTHENTICATOR') !== 'DEFAULT_AUTHENTICATOR',
};

export const SNOWFLAKE_PRIVATE_KEY: RecipeField = {
    name: 'private_key',
    label: 'Private Key',
    helper: 'RSA private key for authentication',
    tooltip:
        'RSA private key for key pair authentication. Paste the entire PEM-formatted private key including BEGIN/END markers.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key',
    placeholder: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----',
    rules: [createAuthTypeValidator('KEY_PAIR_AUTHENTICATOR', 'Private Key', 'Private Key')],
    required: false,
    dynamicHidden: (values) => (values?.authentication_type || 'KEY_PAIR_AUTHENTICATOR') === 'DEFAULT_AUTHENTICATOR',
};

export const SNOWFLAKE_PRIVATE_KEY_PASSWORD: RecipeField = {
    name: 'private_key_password',
    label: 'Private Key Password',
    helper: 'Password if your key is encrypted (optional)',
    tooltip: 'Password for the private key if it is encrypted. Leave blank if your private key is not encrypted.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key_password',
    placeholder: 'Leave blank if key is not encrypted',
    rules: null,
    required: false,
    dynamicHidden: (values) => (values?.authentication_type || 'KEY_PAIR_AUTHENTICATOR') === 'DEFAULT_AUTHENTICATOR',
};

export const SNOWFLAKE_ROLE: RecipeField = {
    name: 'role',
    label: 'Role',
    helper: 'Snowflake Role for metadata',
    tooltip: 'The Role to use when extracting metadata from Snowflake.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.role',
    placeholder: 'datahub_role',
    rules: null,
    required: true,
};
