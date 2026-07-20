import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const TIBCO_EMS_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
    helper: 'URL of the TIBCO EMS REST Proxy admin API',
    required: true,
    tooltip: 'The base URL of the TIBCO EMS REST Proxy, including scheme, host and port.',
    placeholder: 'https://ems-host.example.com:8080',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    rules: null,
};

export const TIBCO_EMS_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    helper: 'Username for HTTP basic authentication',
    tooltip: 'The username used for HTTP basic authentication against the EMS REST Proxy.',
    placeholder: 'admin',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    rules: null,
};

export const TIBCO_EMS_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    helper: 'Password for HTTP basic authentication',
    tooltip: 'The password used for HTTP basic authentication against the EMS REST Proxy.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    rules: null,
};

export const TIBCO_EMS_TOKEN: RecipeField = {
    name: 'token',
    label: 'Bearer Token',
    helper: 'Bearer token used instead of basic auth',
    tooltip: 'An OAuth2 access token or equivalent, used instead of username/password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.token',
    rules: null,
};

export const TIBCO_EMS_VERIFY_SSL: RecipeField = {
    name: 'verify_ssl',
    label: 'Verify SSL',
    helper: 'Verify the server TLS certificate',
    tooltip: 'Whether to verify the server TLS certificate. Disable only for trusted internal endpoints.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.verify_ssl',
    rules: null,
};

export const TIBCO_EMS_INCLUDE_BRIDGES: RecipeField = {
    name: 'include_bridges',
    label: 'Include Bridges',
    helper: 'Emit lineage from EMS bridges',
    tooltip: 'Emit lineage between destinations derived from configured EMS bridges.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_bridges',
    rules: null,
};

export const TIBCO_EMS_INCLUDE_SYSTEM_DESTINATIONS: RecipeField = {
    name: 'include_system_destinations',
    label: 'Include System Destinations',
    helper: 'Ingest internal EMS destinations',
    tooltip: 'Ingest EMS internal destinations (names starting with $sys. or $TMP$). Disabled by default.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_system_destinations',
    rules: null,
};

const queueAllowFieldPath = 'source.config.queue_pattern.allow';
export const TIBCO_EMS_QUEUE_ALLOW: FilterRecipeField = {
    name: 'queue_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific EMS queues',
    tooltip: 'Provide an optional Regular Expression (REGEX) to include specific EMS queue names in ingestion.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: queueAllowFieldPath,
    rules: null,
    section: 'Filter by Queue',
    filteringResource: 'Queue',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, queueAllowFieldPath),
};

const queueDenyFieldPath = 'source.config.queue_pattern.deny';
export const TIBCO_EMS_QUEUE_DENY: FilterRecipeField = {
    name: 'queue_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific EMS queues',
    tooltip: 'Provide an optional Regular Expression (REGEX) to exclude specific EMS queue names from ingestion.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: queueDenyFieldPath,
    rules: null,
    section: 'Filter by Queue',
    filteringResource: 'Queue',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, queueDenyFieldPath),
};

const topicAllowFieldPath = 'source.config.topic_pattern.allow';
export const TIBCO_EMS_TOPIC_ALLOW: FilterRecipeField = {
    name: 'topic_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific EMS topics',
    tooltip: 'Provide an optional Regular Expression (REGEX) to include specific EMS topic names in ingestion.',
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

const topicDenyFieldPath = 'source.config.topic_pattern.deny';
export const TIBCO_EMS_TOPIC_DENY: FilterRecipeField = {
    name: 'topic_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific EMS topics',
    tooltip: 'Provide an optional Regular Expression (REGEX) to exclude specific EMS topic names from ingestion.',
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
