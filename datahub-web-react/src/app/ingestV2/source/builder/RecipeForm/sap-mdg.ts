import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const SAP_MDG_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Gateway Base URL',
    helper: 'Base URL of the SAP Gateway host serving the MDG OData services.',
    tooltip: 'The scheme, host and port of the SAP Gateway, without a trailing service path.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    placeholder: 'https://sap-gw.example.com:44300',
    required: true,
    rules: null,
};

const servicesFieldPath = 'source.config.services';
export const SAP_MDG_SERVICES: RecipeField = {
    name: 'services',
    label: 'OData Service Paths',
    helper: 'OData service paths to ingest, relative to the base URL.',
    tooltip: "Each service's $metadata document is parsed into datasets.",
    type: FieldType.LIST,
    buttonLabel: 'Add service',
    fieldPath: servicesFieldPath,
    placeholder: '/sap/opu/odata/sap/ZMDG_BP_SRV',
    required: true,
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, servicesFieldPath),
};

export const SAP_MDG_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    helper: 'Username for HTTP basic authentication.',
    tooltip: 'Leave blank when authenticating with a bearer token or a client certificate.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'MDG_SERVICE_USER',
    rules: null,
};

export const SAP_MDG_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    helper: 'Password for HTTP basic authentication.',
    tooltip: 'Required together with a username for basic authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
};

export const SAP_MDG_TOKEN: RecipeField = {
    name: 'token',
    label: 'Bearer Token',
    helper: 'Bearer token used instead of basic auth (e.g. an OAuth2 access token).',
    tooltip: 'Provide this instead of a username and password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.token',
    placeholder: 'eyJhbGciOi...',
    rules: null,
};

export const SAP_MDG_CLIENT: RecipeField = {
    name: 'sap_client',
    label: 'SAP Client',
    helper: 'SAP client (sap-client) query parameter appended to every request.',
    tooltip: 'The three-digit SAP client, e.g. 100.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.sap_client',
    placeholder: '100',
    rules: null,
};

export const SAP_MDG_CLIENT_CERT_PATH: RecipeField = {
    name: 'client_certificate_path',
    label: 'Client Certificate Path',
    helper: 'Path to a PEM client certificate for X.509 (mutual TLS) authentication.',
    tooltip: 'Use this when the gateway requires client-certificate authentication.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_certificate_path',
    placeholder: '/certs/mdg-client.pem',
    rules: null,
};

export const SAP_MDG_CLIENT_KEY_PATH: RecipeField = {
    name: 'client_key_path',
    label: 'Client Key Path',
    helper: 'Path to the PEM private key matching the client certificate.',
    tooltip: 'Required alongside the client certificate path.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_key_path',
    placeholder: '/certs/mdg-client-key.pem',
    rules: null,
};

export const SAP_MDG_EMIT_ORPHAN_ENTITY_TYPES: RecipeField = {
    name: 'emit_entity_types_without_sets',
    label: 'Emit Entity Types Without Sets',
    helper: 'Also emit entity types that are not exposed through any entity set.',
    tooltip: 'Disabled by default to keep the catalog aligned with queryable collections.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.emit_entity_types_without_sets',
    rules: null,
};

export const SAP_MDG_INCLUDE_FOREIGN_KEYS: RecipeField = {
    name: 'include_foreign_keys',
    label: 'Include Foreign Keys',
    helper: 'Emit foreign-key constraints derived from OData navigation properties.',
    tooltip: 'Only navigation properties carrying referential constraints produce foreign keys.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_foreign_keys',
    rules: null,
};

const entitySetAllowFieldPath = 'source.config.entity_set_pattern.allow';
export const SAP_MDG_ENTITY_SET_ALLOW: FilterRecipeField = {
    name: 'entity_set_pattern.allow',
    label: 'Entity Set Allow Patterns',
    tooltip: 'Only include OData entity sets whose name matches these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: entitySetAllowFieldPath,
    rules: null,
    section: 'Entity Sets',
    rule: FilterRule.INCLUDE,
    filteringResource: 'Entity Set',
    placeholder: '.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, entitySetAllowFieldPath),
};

const entitySetDenyFieldPath = 'source.config.entity_set_pattern.deny';
export const SAP_MDG_ENTITY_SET_DENY: FilterRecipeField = {
    name: 'entity_set_pattern.deny',
    label: 'Entity Set Deny Patterns',
    tooltip: 'Exclude OData entity sets whose name matches these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: entitySetDenyFieldPath,
    rules: null,
    section: 'Entity Sets',
    rule: FilterRule.EXCLUDE,
    filteringResource: 'Entity Set',
    placeholder: 'Z_TEST_.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, entitySetDenyFieldPath),
};
