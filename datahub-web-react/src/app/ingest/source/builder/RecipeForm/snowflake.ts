import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

/**
 * Helper function to determine if a Snowflake field should be visible based on authentication type.
 * Used for conditional field rendering in forms.
 */
export function shouldShowSnowflakeField(fieldName: string, authenticationType?: string): boolean {
    // Hide password when using key pair authentication
    if (fieldName === 'password' && authenticationType === 'KEY_PAIR_AUTHENTICATOR') {
        return false;
    }
    // Hide private key fields when using default (username/password) authentication
    if (
        (fieldName === 'private_key' || fieldName === 'private_key_password') &&
        authenticationType === 'DEFAULT_AUTHENTICATOR'
    ) {
        return false;
    }
    return true;
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
    tooltip: 'Snowflake password. Required when using Username & Password authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: [
        ({ getFieldValue }) => ({
            validator(_, value) {
                const authType = getFieldValue('authentication_type');
                // Require password for default (username/password) authentication
                if (authType === 'DEFAULT_AUTHENTICATOR' && !value) {
                    return Promise.reject(new Error('Password is required for Username & Password authentication'));
                }
                return Promise.resolve();
            },
        }),
    ],
    required: false,
    shouldShow: (formValues) => shouldShowSnowflakeField('password', formValues.authentication_type),
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
        { label: 'Username & Password', value: 'DEFAULT_AUTHENTICATOR' },
        { label: 'Private Key', value: 'KEY_PAIR_AUTHENTICATOR' },
    ],
    placeholder: 'DEFAULT_AUTHENTICATOR',
    rules: null,
    required: true,
};

export const SNOWFLAKE_PRIVATE_KEY: RecipeField = {
    name: 'private_key',
    label: 'Private Key',
    tooltip:
        'RSA private key for key pair authentication. Can be provided as a string or path to key file. Required when using Private Key authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key',
    placeholder: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----',
    rules: [
        ({ getFieldValue }) => ({
            validator(_, value) {
                const authType = getFieldValue('authentication_type');
                // Require private key for key pair authentication
                if (authType === 'KEY_PAIR_AUTHENTICATOR' && !value) {
                    return Promise.reject(new Error('Private Key is required for Private Key authentication'));
                }
                return Promise.resolve();
            },
        }),
    ],
    required: false,
    shouldShow: (formValues) => shouldShowSnowflakeField('private_key', formValues.authentication_type),
};

export const SNOWFLAKE_PRIVATE_KEY_PASSWORD: RecipeField = {
    name: 'private_key_password',
    label: 'Private Key Password',
    tooltip: 'Password for the private key if it is encrypted.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key_password',
    placeholder: 'private_key_password',
    rules: null,
    required: false,
    shouldShow: (formValues) => shouldShowSnowflakeField('private_key_password', formValues.authentication_type),
};
