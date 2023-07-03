import { RecipeField, FieldType } from './common';

export const TRINO = 'trino';

export const TRINO_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Trino is running. For example, 'trino-server:5432'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'trino-server:5432',
    required: true,
    rules: null,
};

export const TRINO_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'my_db',
    required: true,
    rules: null,
};

export const TRINO_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The Trino username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'trino',
    required: true,
    rules: null,
};

export const TRINO_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The Trino password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};
