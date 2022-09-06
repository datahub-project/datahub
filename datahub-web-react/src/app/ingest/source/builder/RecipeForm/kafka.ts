import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

const saslUsernameFieldPath = ['source', 'config', 'connection', 'consumer_config', 'sasl.username'];
export const KAFKA_SASL_USERNAME: RecipeField = {
    name: 'connection.consumer_config.sasl.username',
    label: 'Username',
    tooltip: 'SASL username. You can get (in the Confluent UI) from your cluster -> Data Integration -> API Keys.',
    type: FieldType.TEXT,
    fieldPath: saslUsernameFieldPath,
    rules: null,
};

const saslPasswordFieldPath = ['source', 'config', 'connection', 'consumer_config', 'sasl.password'];
export const KAFKA_SASL_PASSWORD: RecipeField = {
    name: 'connection.consumer_config.sasl.password',
    label: 'Password',
    tooltip: 'SASL password. You can get (in the Confluent UI) from your cluster -> Data Integration -> API Keys.',
    type: FieldType.SECRET,
    fieldPath: saslPasswordFieldPath,
    rules: null,
};

export const KAFKA_BOOTSTRAP: RecipeField = {
    name: 'connection.bootstrap',
    label: 'Connection Bootstrap',
    tooltip: 'Bootstrap URL.',
    placeholder: 'abc-defg.eu-west-1.aws.confluent.cloud:9092',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connection.bootstrap',
    rules: null,
};

export const KAFKA_SCHEMA_REGISTRY_URL: RecipeField = {
    name: 'connection.schema_registry_url',
    label: 'Schema Registry URL',
    tooltip: 'URL where your Confluent Cloud Schema Registry is hosted.',
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
    tooltip:
        'API credentials for Confluent schema registry which you get (in Confluent UI) from Schema Registry -> API credentials.',
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
    tooltip: 'Security Protocol',
    type: FieldType.SELECT,
    fieldPath: securityProtocolFieldPath,
    rules: null,
    options: [
        { label: 'SASL_SSL', value: 'SASL_SSL' },
        { label: 'SASL_PLAINTEXT', value: 'SASL_PLAINTEXT' },
    ],
};

const saslMechanismFieldPath = ['source', 'config', 'connection', 'consumer_config', 'sasl.mechanism'];
export const KAFKA_SASL_MECHANISM: RecipeField = {
    name: 'sasl.mechanism',
    label: 'SASL Mechanism',
    tooltip: 'SASL Mechanism',
    type: FieldType.SELECT,
    fieldPath: saslMechanismFieldPath,
    rules: null,
    options: [
        { label: 'PLAIN', value: 'PLAIN' },
        { label: 'SCRAM-SHA-256', value: 'SCRAM-SHA-256' },
        { label: 'SCRAM-SHA-512', value: 'SCRAM-SHA-512' },
    ],
};

const topicAllowFieldPath = 'source.config.topic_patterns.allow';
export const TOPIC_ALLOW: RecipeField = {
    name: 'topic_patterns.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: topicAllowFieldPath,
    rules: null,
    section: 'Topics',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, topicAllowFieldPath),
};

const topicDenyFieldPath = 'source.config.topic_patterns.deny';
export const TOPIC_DENY: RecipeField = {
    name: 'topic_patterns.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: topicDenyFieldPath,
    rules: null,
    section: 'Topics',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, topicDenyFieldPath),
};
