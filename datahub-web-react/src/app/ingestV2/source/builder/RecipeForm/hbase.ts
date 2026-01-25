import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const HBASE_HOST: RecipeField = {
    name: 'host',
    label: 'Host',
    tooltip: 'HBase Thrift server hostname or IP address',
    type: FieldType.TEXT,
    fieldPath: 'source.config.host',
    placeholder: 'localhost',
    required: true,
    rules: null,
};

export const HBASE_PORT: RecipeField = {
    name: 'port',
    label: 'Port',
    tooltip: 'HBase Thrift server port (default: 9090 for Thrift1)',
    type: FieldType.TEXT,
    fieldPath: 'source.config.port',
    placeholder: '9090',
    required: false,
    rules: null,
};

export const HBASE_USE_SSL: RecipeField = {
    name: 'use_ssl',
    label: 'Use SSL',
    tooltip: 'Whether to use SSL/TLS for connection',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.use_ssl',
    required: false,
    rules: null,
};

export const HBASE_AUTH_MECHANISM: RecipeField = {
    name: 'auth_mechanism',
    label: 'Authentication Mechanism',
    tooltip: 'Authentication mechanism (None, KERBEROS, or custom)',
    type: FieldType.TEXT,
    fieldPath: 'source.config.auth_mechanism',
    placeholder: 'KERBEROS',
    required: false,
    rules: null,
};

export const NAMESPACE_ALLOW: RecipeField = {
    name: 'namespace_pattern.allow',
    label: 'Allow Patterns for Namespace',
    tooltip:
        'Use regex here. e.g. to allow all namespaces, use ".*" or to allow namespaces starting with "production" use "production.*"',
    placeholder: '.*',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.namespace_pattern.allow',
    rules: null,
    section: 'Namespaces',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'source.config.namespace_pattern.allow'),
};

export const NAMESPACE_DENY: RecipeField = {
    name: 'namespace_pattern.deny',
    label: 'Deny Patterns for Namespace',
    tooltip:
        'Use regex here. Deny patterns take precedence over allow patterns. e.g. to deny all system namespaces, use "system.*"',
    placeholder: 'system.*',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.namespace_pattern.deny',
    rules: null,
    section: 'Namespaces',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'source.config.namespace_pattern.deny'),
};

export const HBASE_INCLUDE_COLUMN_FAMILIES: RecipeField = {
    name: 'include_column_families',
    label: 'Include Column Families',
    tooltip: 'Whether to include column families as schema metadata',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_column_families',
    required: false,
    rules: null,
};

export const HBASE_MAX_COLUMN_QUALIFIERS: RecipeField = {
    name: 'max_column_qualifiers',
    label: 'Max Column Qualifiers',
    tooltip: 'Maximum number of column qualifiers to sample per column family',
    type: FieldType.TEXT,
    fieldPath: 'source.config.max_column_qualifiers',
    placeholder: '100',
    required: false,
    rules: null,
};
