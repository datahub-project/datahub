import { RecipeField, FieldType } from './common';

export const HIVE_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Hive is running. For example, 'hive:9083'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'hive:9083',
    required: true,
    rules: null,
};

export const HIVE_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database. If left blank, metadata for all databases will be extracted.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'my_db',
    rules: null,
};

export const HIVE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The Hive username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'hive',
    required: true,
    rules: null,
};

export const HIVE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The Hive password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};
