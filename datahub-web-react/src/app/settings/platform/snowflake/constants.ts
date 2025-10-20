import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const SNOWFLAKE_CONNECTION_NAME = {
    name: 'name',
    label: 'Connection Name',
    tooltip: 'An internal name to idenity this connection in DataHub.',
    type: FieldType.TEXT,
    fieldPath: 'name',
    placeholder: 'Snowflake Connection',
    rules: null,
    required: true,
};

export const SNOWFLAKE_ACCOUNT_ID = {
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

export const SNOWFLAKE_WAREHOUSE = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'The name of the Snowflake Warehouse to extract metadata from.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse',
    placeholder: 'COMPUTE_WH',
    rules: null,
    required: true,
};

export const SNOWFLAKE_USERNAME = {
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
    shouldShow: (formValues) => formValues.authentication_type === 'DEFAULT_AUTHENTICATOR',
};

export const SNOWFLAKE_ROLE = {
    name: 'role',
    label: 'Role',
    tooltip: 'The Role to use when extracting metadata from Snowflake.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.role',
    placeholder: 'datahub_role',
    rules: null,
    required: true,
};

export const SNOWFLAKE_DATABASE = {
    name: 'database',
    label: 'Database',
    tooltip: null,
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'Optional - select a specific database to propagate tags to',
    rules: null,
    required: false,
};

export const SNOWFLAKE_SCHEMA = {
    name: 'schema',
    label: 'Schema',
    tooltip: null,
    type: FieldType.TEXT,
    fieldPath: 'source.config.schema',
    placeholder: 'Optional - select a specific schema to propagate tags to',
    rules: null,
    required: false,
};

export const SNOWFLAKE_AUTHENTICATION_TYPE = {
    name: 'authentication_type',
    label: 'Authentication Type',
    tooltip: 'The type of authenticator to use when connecting to Snowflake.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.authentication_type',
    placeholder: 'DEFAULT_AUTHENTICATOR',
    options: [
        { label: 'Username/Password', value: 'DEFAULT_AUTHENTICATOR' },
        { label: 'Private Key', value: 'KEY_PAIR_AUTHENTICATOR' },
    ],
    rules: null,
    required: true,
};

export const SNOWFLAKE_PRIVATE_KEY: RecipeField = {
    name: 'private_key',
    label: 'Private Key',
    tooltip: 'Private key in PEM format for key pair authentication. Should start with -----BEGIN PRIVATE KEY-----',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key',
    placeholder: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----',
    rules: null,
    required: false,
    shouldShow: (formValues) => formValues.authentication_type === 'KEY_PAIR_AUTHENTICATOR',
};

export const SNOWFLAKE_PRIVATE_KEY_PASSWORD: RecipeField = {
    name: 'private_key_password',
    label: 'Private Key Password',
    tooltip: 'Password for your private key. Required if using key pair authentication with encrypted private key.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.private_key_password',
    placeholder: 'Private key password (if encrypted)',
    rules: null,
    required: false,
    shouldShow: (formValues) => formValues.authentication_type === 'KEY_PAIR_AUTHENTICATOR',
};

export const fields: RecipeField[] = [
    SNOWFLAKE_CONNECTION_NAME,
    SNOWFLAKE_ACCOUNT_ID,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_USERNAME,
    SNOWFLAKE_AUTHENTICATION_TYPE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_PRIVATE_KEY_PASSWORD,
    SNOWFLAKE_ROLE,
    // SNOWFLAKE_DATABASE, - Not yet supported on backend.
    // SNOWFLAKE_SCHEMA, - Not yet supported on backend.
];
