import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const TIDB = 'tidb';

export const TIDB_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    helper: 'TiDB host and port',
    tooltip:
        "The host and port where TiDB is running. For example, 'tidb-server:4000'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'tidb-server:4000',
    required: true,
    rules: null,
};

export const TIDB_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    helper: 'Specific Database to ingest',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'my_db',
    required: true,
    rules: null,
};

export const TIDB_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    helper: 'TiDB username for metadata',
    tooltip: 'The TiDB username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'tidb',
    required: true,
    rules: null,
};

export const TIDB_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    helper: 'TiDB password for user',
    tooltip: 'The TiDB password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};
