import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const DORIS = 'doris';

export const DORIS_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Apache Doris is running. For example, 'doris-server:9030'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'doris-server:9030',
    required: true,
    rules: null,
};

export const DORIS_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'my_db',
    required: true,
    rules: null,
};

export const DORIS_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The Apache Doris username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'root',
    required: true,
    rules: null,
};

export const DORIS_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The Apache Doris password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};
