import { RecipeField, FieldType } from './common';

export const POSTGRES_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host Port',
    tooltip: 'host URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    rules: null,
};

export const POSTGRES_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Database (catalog). Optional, if not specified, ingests from all databases.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    rules: null,
};

export const POSTGRES_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Username',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    rules: null,
};

export const POSTGRES_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Password',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    rules: null,
};
