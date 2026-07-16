import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

// ---- Connection Section ----

export const DATAPLEX_PROJECT_IDS: RecipeField = {
    name: 'project_ids',
    label: 'Project IDs',
    tooltip: 'Explicit list of GCP project IDs to ingest. Skips Resource Manager API calls for project discovery.',
    type: FieldType.LIST,
    buttonLabel: 'Add Project ID',
    fieldPath: 'source.config.project_ids',
    placeholder: 'my-gcp-project',
    rules: null,
    required: true,
};

export const DATAPLEX_PRIVATE_KEY_ID: RecipeField = {
    name: 'credential.private_key_id',
    label: 'Private Key Id',
    helper: 'Private Key ID from GCP service account',
    tooltip: "The Private Key ID from your GCP service account's JSON key (private_key_id).",
    type: FieldType.SECRET,
    fieldPath: 'source.config.credential.private_key_id',
    placeholder: 'd0121d0000882411234e11166c6aaa23ed5d74e0',
    rules: null,
    required: true,
};

export const DATAPLEX_PRIVATE_KEY: RecipeField = {
    name: 'credential.private_key',
    label: 'Private Key',
    helper: 'Private key from GCP service account',
    tooltip: "The Private key from your GCP service account's JSON key (private_key).",
    placeholder: '-----BEGIN PRIVATE KEY-----....\n-----END PRIVATE KEY-----',
    type: FieldType.SECRET,
    fieldPath: 'source.config.credential.private_key',
    rules: null,
    required: true,
};

export const DATAPLEX_CLIENT_EMAIL: RecipeField = {
    name: 'credential.client_email',
    label: 'Client Email',
    helper: 'Client email from GCP service account',
    tooltip: "The Client Email from your GCP service account's JSON key (client_email).",
    placeholder: 'service-account@my-project.iam.gserviceaccount.com',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.client_email',
    rules: null,
    required: true,
};

export const DATAPLEX_CLIENT_ID: RecipeField = {
    name: 'credential.client_id',
    label: 'Client ID',
    helper: 'Client ID from GCP service account',
    tooltip: "The Client ID from your GCP service account's JSON key (client_id).",
    placeholder: '123456789098765432101',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.client_id',
    rules: null,
    required: true,
};

// ---- Filter Section ----

const projectIdAllowFieldPath = 'source.config.project_id_pattern.allow';
export const DATAPLEX_PROJECT_ALLOW: FilterRecipeField = {
    name: 'project_id_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Filter for project IDs',
    tooltip: 'Use regex to include specific GCP project IDs.',
    placeholder: '^prod-.*',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectIdAllowFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectIdAllowFieldPath),
};

const projectIdDenyFieldPath = 'source.config.project_id_pattern.deny';
export const DATAPLEX_PROJECT_DENY: FilterRecipeField = {
    name: 'project_id_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Filter out project IDs',
    tooltip: 'Use regex to exclude specific GCP project IDs.',
    placeholder: '.*-sandbox$',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectIdDenyFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectIdDenyFieldPath),
};

const entryAllowFieldPath = 'source.config.filter_config.entries.pattern.allow';
export const DATAPLEX_ENTRY_ALLOW: FilterRecipeField = {
    name: 'filter_config.entries.pattern.allow',
    label: 'Allow Patterns',
    helper: 'Filter for entry names',
    tooltip: 'Use regex to include specific Dataplex entry names.',
    placeholder: '^bq_.*',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: entryAllowFieldPath,
    rules: null,
    section: 'Entries',
    filteringResource: 'Entry',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, entryAllowFieldPath),
};

const entryDenyFieldPath = 'source.config.filter_config.entries.pattern.deny';
export const DATAPLEX_ENTRY_DENY: FilterRecipeField = {
    name: 'filter_config.entries.pattern.deny',
    label: 'Deny Patterns',
    helper: 'Filter out entry names',
    tooltip: 'Use regex to exclude specific Dataplex entry names.',
    placeholder: '.*_temp$',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: entryDenyFieldPath,
    rules: null,
    section: 'Entries',
    filteringResource: 'Entry',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, entryDenyFieldPath),
};

const entryGroupAllowFieldPath = 'source.config.filter_config.entry_groups.pattern.allow';
export const DATAPLEX_ENTRY_GROUP_ALLOW: FilterRecipeField = {
    name: 'filter_config.entry_groups.pattern.allow',
    label: 'Allow Patterns',
    helper: 'Filter for entry groups',
    tooltip: 'Use regex to include specific Dataplex entry group resource names.',
    placeholder: '^prod_.*',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: entryGroupAllowFieldPath,
    rules: null,
    section: 'Entry Groups',
    filteringResource: 'Entry Group',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, entryGroupAllowFieldPath),
};

const entryGroupDenyFieldPath = 'source.config.filter_config.entry_groups.pattern.deny';
export const DATAPLEX_ENTRY_GROUP_DENY: FilterRecipeField = {
    name: 'filter_config.entry_groups.pattern.deny',
    label: 'Deny Patterns',
    helper: 'Filter out entry groups',
    tooltip: 'Use regex to exclude specific Dataplex entry group resource names.',
    placeholder: '.*_test$',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: entryGroupDenyFieldPath,
    rules: null,
    section: 'Entry Groups',
    filteringResource: 'Entry Group',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, entryGroupDenyFieldPath),
};

// ---- Advanced Section ----

export const DATAPLEX_ENTRIES_LOCATIONS: RecipeField = {
    name: 'entries_locations',
    label: 'Entries Locations',
    tooltip:
        'GCP regions to scan for Universal Catalog entries. Supports multi-regions (us, eu, asia) and single regions (us-central1, etc.).',
    type: FieldType.LIST,
    buttonLabel: 'Add location',
    fieldPath: 'source.config.entries_locations',
    placeholder: 'us',
    rules: null,
};

export const DATAPLEX_INCLUDE_SCHEMA: RecipeField = {
    name: 'include_schema',
    label: 'Include Schema',
    tooltip: 'Extract and ingest schema metadata (columns, types, descriptions). Disable for faster ingestion.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_schema',
    rules: null,
};

export const DATAPLEX_INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    label: 'Include Lineage',
    tooltip: 'Extract table-level lineage from the Dataplex Lineage API.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_lineage',
    rules: null,
};

export const DATAPLEX_INCLUDE_GLOSSARIES: RecipeField = {
    name: 'include_glossaries',
    label: 'Include Glossaries',
    tooltip: 'Ingest Dataplex Business Glossary entities as DataHub GlossaryNodes and GlossaryTerms.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_glossaries',
    rules: null,
};

export const DATAPLEX_INCLUDE_GLOSSARY_TERM_ASSOCIATIONS: RecipeField = {
    name: 'include_glossary_term_associations',
    label: 'Include Glossary Term Associations',
    tooltip:
        'Ingest term-to-asset associations via Dataplex lookupEntryLinks API. Requires resourcemanager.projects.get role on all projects.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_glossary_term_associations',
    rules: null,
};

export const DATAPLEX_LINEAGE_LOCATIONS: RecipeField = {
    name: 'lineage_locations',
    label: 'Lineage Locations',
    tooltip:
        'GCP regions to scan for lineage data. Narrowing from the default (all 73 regions) is critical for performance.',
    type: FieldType.LIST,
    buttonLabel: 'Add location',
    fieldPath: 'source.config.lineage_locations',
    placeholder: 'us-central1',
    rules: null,
};

export const DATAPLEX_GLOSSARY_LOCATIONS: RecipeField = {
    name: 'glossary_locations',
    label: 'Glossary Locations',
    tooltip: 'GCP locations to scan for Business Glossaries. Glossaries typically exist in "global".',
    type: FieldType.LIST,
    buttonLabel: 'Add location',
    fieldPath: 'source.config.glossary_locations',
    placeholder: 'global',
    rules: null,
};
