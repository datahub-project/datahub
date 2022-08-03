import { RecipeField, FieldType } from './common';

export const KAFKA_SASL_USERNAME: RecipeField = {
    name: 'connection.consumer_config.sasl.username',
    label: 'Username',
    tooltip: 'SASL username. You can get (in the Confluent UI) from your cluster -> Data Integration -> API Keys.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connection.consumer_config.sasl.username',
    rules: null,
};

export const KAFKA_SASL_PASSWORD: RecipeField = {
    name: 'connection.consumer_config.sasl.password',
    label: 'Password',
    tooltip: 'SASL password. You can get (in the Confluent UI) from your cluster -> Data Integration -> API Keys.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connection.consumer_config.sasl.password',
    rules: null,
};

export const KAFKA_BOOTSTRAP: RecipeField = {
    name: 'connection.bootstrap',
    label: 'Connection Bootstrap',
    tooltip: 'Bootstrap URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connection.bootstrap',
    rules: null,
};

export const KAFKA_SCHEMA_REGISTRY_URL: RecipeField = {
    name: 'connection.schema_registry_url',
    label: 'Schema Registry URL',
    tooltip: 'URL where your Confluent Cloud Schema Registry is hosted.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connection.schema_registry_url',
    rules: null,
};

export const KAFKA_SCHEMA_REGISTRY_USER_CREDENTIAL: RecipeField = {
    name: 'schema_registry_config.basic.auth.user.info',
    label: 'Schema Registry Credentials',
    tooltip:
        'API credentials for Confluent schema registry which you get (in Confluent UI) from Schema Registry -> API credentials.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.schema_registry_config.basic.auth.user.info',
    rules: null,
};

export const KAFKA_SECURITY_PROTOCOL: RecipeField = {
    name: 'security.protocol',
    label: 'Security Protocol',
    tooltip: 'Security Protocol',
    type: FieldType.SELECT,
    fieldPath: 'source.config.security.protocol',
    rules: null,
    options: [
        { label: 'SASL_SSL', value: 'SASL_SSL' },
        { label: 'SCRAM-SHA-256', value: 'SCRAM-SHA-256' },
        { label: 'SCRAM-SHA-512', value: 'SCRAM-SHA-512' },
    ],
};
