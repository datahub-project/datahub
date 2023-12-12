import { RecipeField, FieldType } from './common';

export const MYSQL_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Postgres is running. For example, 'localhost:5432'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'mysql:5432',
    required: true,
    rules: null,
};

export const MYSQL_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The MySQL username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'mysql',
    required: true,
    rules: null,
};

export const MYSQL_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The MySQL password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};
