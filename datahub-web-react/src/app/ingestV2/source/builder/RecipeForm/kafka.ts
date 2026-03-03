import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

// TODO: Currently platform_instance is required to be present for stateful ingestion to work
// We need to solve this prior to enabling by default here.

const saslUsernameFieldPath = ['source', 'config', 'connection', 'consumer_config', 'sasl.username'];
export const KAFKA_SASL_USERNAME: RecipeField = {
    name: 'connection.consumer_config.sasl.username',
    label: 'Username',
    helper: 'SASL username for authentication',
    placeholder: 'datahub-client',
    tooltip:
        'The SASL username. Required if the Security Protocol is SASL based. In the Confluent Control Center, you can find this in Cluster > Data Integration > API Keys.',
    type: FieldType.TEXT,
    fieldPath: saslUsernameFieldPath,
    rules: null,
};

const saslPasswordFieldPath = ['source', 'config', 'connection', 'consumer_config', 'sasl.password'];
export const KAFKA_SASL_PASSWORD: RecipeField = {
    name: 'connection.consumer_config.sasl.password',
    label: 'Password',
    helper: 'SASL password for authentication',
    placeholder: 'datahub-client-password',
    tooltip:
        'The SASL Password. Required if the Security Protocol is SASL based. In the Confluent Control Center, you can find this in Cluster > Data Integration > API Keys.',
    type: FieldType.SECRET,
    fieldPath: saslPasswordFieldPath,
    rules: null,
};

export const KAFKA_BOOTSTRAP: RecipeField = {
    name: 'connection.bootstrap',
    label: 'Bootstrap Servers',
    helper: 'Host and port for cluster metadata',
    required: true,
    tooltip:
        'The ‘host[:port]’ string (or list of ‘host[:port]’ strings) that we should contact to bootstrap initial cluster metadata.',
    placeholder: 'abc-defg.eu-west-1.aws.confluent.cloud:9092',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connection.bootstrap',
    rules: null,
};

export const KAFKA_SCHEMA_REGISTRY_URL: RecipeField = {
    name: 'connection.schema_registry_url',
    label: 'Schema Registry URL',
    helper: 'URL for schema registry',
    tooltip:
        'The URL where the schema Schema Registry is hosted. If provided, DataHub will attempt to extract Avro and Protobuf topic schemas from the registry.',
    placeholder: 'https://abc-defgh.us-east-2.aws.confluent.cloud',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connection.schema_registry_url',
    rules: null,
};

const registryCredentialsFieldPath = [
    'source',
    'config',
    'connection',
    'schema_registry_config',
    'basic.auth.user.info',
];
export const KAFKA_SCHEMA_REGISTRY_USER_CREDENTIAL: RecipeField = {
    name: 'schema_registry_config.basic.auth.user.info',
    label: 'Schema Registry Credentials',
    helper: 'API credentials for Schema Registry',
    tooltip:
        'API credentials for the Schema Registry. In Confluent Control Center, you can find these under Schema Registry > API Credentials.',
    // eslint-disable-next-line no-template-curly-in-string
    placeholder: '${REGISTRY_API_KEY_ID}:${REGISTRY_API_KEY_SECRET}',
    type: FieldType.TEXT,
    fieldPath: registryCredentialsFieldPath,
    rules: null,
};

const securityProtocolFieldPath = ['source', 'config', 'connection', 'consumer_config', 'security.protocol'];
export const KAFKA_SECURITY_PROTOCOL: RecipeField = {
    name: 'security.protocol',
    label: 'Security Protocol',
    helper: 'Security protocol for authentication',
    tooltip: 'The Security Protocol used for authentication.',
    type: FieldType.SELECT,
    required: true,
    fieldPath: securityProtocolFieldPath,
    rules: null,
    options: [
        { label: 'PLAINTEXT', value: 'PLAINTEXT' },
        { label: 'SASL_SSL', value: 'SASL_SSL' },
        { label: 'SASL_PLAINTEXT', value: 'SASL_PLAINTEXT' },
        { label: 'SSL', value: 'SSL' },
    ],
};

const saslMechanismFieldPath = ['source', 'config', 'connection', 'consumer_config', 'sasl.mechanism'];
export const KAFKA_SASL_MECHANISM: RecipeField = {
    name: 'sasl.mechanism',
    label: 'SASL Mechanism',
    helper: 'SASL mechanism for authentication',
    tooltip:
        'The SASL mechanism used for authentication. This field is required if the selected Security Protocol is SASL based.',
    type: FieldType.SELECT,
    fieldPath: saslMechanismFieldPath,
    placeholder: 'None',
    rules: null,
    options: [
        { label: 'PLAIN', value: 'PLAIN' },
        { label: 'SCRAM-SHA-256', value: 'SCRAM-SHA-256' },
        { label: 'SCRAM-SHA-512', value: 'SCRAM-SHA-512' },
    ],
};

const topicAllowFieldPath = 'source.config.topic_patterns.allow';
export const TOPIC_ALLOW: FilterRecipeField = {
    name: 'topic_patterns.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Kafka topics',
    tooltip: 'Provide an optional Regular Expression (REGEX) to include specific Kafka Topic names in ingestion.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: topicAllowFieldPath,
    rules: null,
    section: 'Filter by Topic',
    filteringResource: 'Topic',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, topicAllowFieldPath),
};

const topicDenyFieldPath = 'source.config.topic_patterns.deny';
export const TOPIC_DENY: FilterRecipeField = {
    name: 'topic_patterns.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Kafka topics',
    tooltip: 'Provide an optional Regular Expression (REGEX) to exclude specific Kafka Topic names from ingestion.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: topicDenyFieldPath,
    rules: null,
    section: 'Filter by Topic',
    filteringResource: 'Topic',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, topicDenyFieldPath),
};
