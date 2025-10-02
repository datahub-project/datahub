import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

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
    required: false,
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
    tooltip: 'RSA private key for key pair authentication. Can be provided as a string or path to key file.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key',
    placeholder: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----',
    rules: null,
    required: false,
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
};
