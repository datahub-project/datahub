import { RecipeField, FieldType } from './common';

export const VERTICA = 'trino';

export const VERTICA_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Vertics is running. For example, 'vertica_ip:5433'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'vertica_ip:5433',
    required: true,
    rules: null,
};

export const VERTICA_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'my_db',
    required: true,
    rules: null,
};

export const VERTICA_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The Vertica username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'Vertica',
    required: true,
    rules: null,
};

export const VERTICA_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The Vertica password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};