import { FieldType } from '../../../ingest/source/builder/RecipeForm/common';

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

export const SNOWFLAKE_PASSWORD = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
    required: true,
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

export const fields = [
    SNOWFLAKE_CONNECTION_NAME,
    SNOWFLAKE_ACCOUNT_ID,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_USERNAME,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    // SNOWFLAKE_DATABASE, - Not yet supported on backend.
    // SNOWFLAKE_SCHEMA, - Not yet supported on backend.
];
