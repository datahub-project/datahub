import { RecipeField, FieldType } from './common';

export const POSTGRES_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Postgres is running. For example, 'postgres:5432'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'postgres:5432',
    required: true,
    rules: null,
};

export const POSTGRES_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'my_db',
    required: true,
    rules: null,
};

export const POSTGRES_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The Postgres username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'postgres',
    required: true,
    rules: null,
};

export const POSTGRES_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The Postgres password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};
