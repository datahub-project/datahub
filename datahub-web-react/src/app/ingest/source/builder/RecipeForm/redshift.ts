import { RecipeField, FieldType } from './common';

export const REDSHIFT_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Redshift is running. For example, 'redshift:5439'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'redshift.company.us-west-1.redshift.amazonaws.com:5439',
    rules: null,
    required: true,
};

export const REDSHIFT_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'database_name',
    rules: null,
    required: true,
};

export const REDSHIFT_USERNAME: RecipeField = {
    name: 'redshift.username',
    label: 'Username',
    tooltip: 'A Redshift username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'redshift',
    rules: null,
    required: true,
};

export const REDSHIFT_PASSWORD: RecipeField = {
    name: 'redshift.password',
    label: 'Password',
    tooltip: 'The password of the username.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
    required: true,
};
