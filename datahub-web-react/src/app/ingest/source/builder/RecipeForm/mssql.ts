import { RecipeField, FieldType } from './common';

export const MSSQL = 'mssql';

export const MSSQL_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where SQL Server is running. For example, 'mssql-server:5432'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'mssql-server:5432',
    required: true,
    rules: null,
};

export const MSSQL_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'my_db',
    required: true,
    rules: null,
};

export const MSSQL_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The SQL Server username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'mssql',
    required: true,
    rules: null,
};

export const MSSQL_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The SQL Server password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};
