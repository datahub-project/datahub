/**
 * BigID Recipe Form Fields (V1 Ingestion UI) — SOURCE OF TRUTH for the shared, non-filter
 * fields. The V2 form (`ingestV2/source/builder/RecipeForm/bigid.ts`) re-uses these exports and
 * only redefines the connection allow/deny fields (V2 uses FilterRecipeField, which V1 lacks).
 * Add or edit a plain field here and V2 picks it up automatically — no need to touch both files.
 *
 * IMPORTANT: For per-connection platform overrides (datasource_platform_mapping), use the
 * YAML editor mode. That nested structure maps BigID connection names to DataHub platforms,
 * environments, and platform instances and is best expressed as YAML.
 */
import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

export const BIGID_URL: RecipeField = {
    name: 'bigid_url',
    label: 'BigID URL',
    tooltip: 'Base URL of the BigID instance, e.g. https://bigid.example.com.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.bigid_url',
    placeholder: 'https://bigid.example.com',
    rules: null,
    required: true,
};

export const BIGID_USER_TOKEN: RecipeField = {
    name: 'user_token',
    label: 'User Token',
    tooltip: 'Long-lived BigID user token. Exchanged for a short-lived access token at startup.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.user_token',
    placeholder: 'your-bigid-user-token',
    rules: null,
};

export const BIGID_ACCESS_TOKEN: RecipeField = {
    name: 'access_token',
    label: 'Access Token',
    tooltip: 'Short-lived BigID access token. Used directly; provide either this or a User Token.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.access_token',
    placeholder: 'your-bigid-access-token',
    rules: null,
};

export const BIGID_ENV: RecipeField = {
    name: 'env',
    label: 'Environment',
    tooltip: 'The environment for all emitted metadata (e.g., PROD, DEV, STAGING).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.env',
    placeholder: 'PROD',
    rules: null,
};

export const BIGID_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    tooltip: 'Unique identifier for this BigID instance. Useful when ingesting from multiple BigID accounts.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.platform_instance',
    placeholder: 'bigid-prod',
    rules: null,
};

const bigidConnectionAllowFieldPath = 'source.config.connection_pattern.allow';
export const BIGID_CONNECTION_ALLOW: RecipeField = {
    name: 'connection_pattern.allow',
    label: 'Connection Allow Patterns',
    tooltip: 'Only include BigID connections (data sources) whose name matches these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: bigidConnectionAllowFieldPath,
    rules: null,
    section: 'Connections',
    placeholder: '.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, bigidConnectionAllowFieldPath),
};

const bigidConnectionDenyFieldPath = 'source.config.connection_pattern.deny';
export const BIGID_CONNECTION_DENY: RecipeField = {
    name: 'connection_pattern.deny',
    label: 'Connection Deny Patterns',
    tooltip: 'Exclude BigID connections (data sources) whose name matches these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: bigidConnectionDenyFieldPath,
    rules: null,
    section: 'Connections',
    placeholder: 'sandbox-.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, bigidConnectionDenyFieldPath),
};

export const BIGID_MIN_CONFIDENCE: RecipeField = {
    name: 'minimum_confidence_threshold',
    label: 'Minimum Confidence Threshold',
    tooltip:
        'Only sync classification findings at or above this confidence (0.0–1.0). ' +
        'BigID ranks map to HIGH = 0.75, MEDIUM = 0.50, LOW = 0.25.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.minimum_confidence_threshold',
    placeholder: '0.5',
    rules: null,
    section: 'Advanced',
};

export const BIGID_CREATE_DATASETS: RecipeField = {
    name: 'create_datasets',
    label: 'Create Datasets',
    tooltip:
        'Emit Dataset + SchemaMetadata for objects not yet in DataHub. ' +
        'Leave off for pure enrichment mode (only enriches existing datasets).',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.create_datasets',
    rules: null,
    section: 'Advanced',
};

export const BIGID_CONFIDENCE_LEVEL_TAG: RecipeField = {
    name: 'confidence_level_tag',
    label: 'Emit Confidence-Level Tags',
    tooltip: 'Emit a bigid.confidence:{LEVEL} tag alongside each GlossaryTerm on a column.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.confidence_level_tag',
    rules: null,
    section: 'Advanced',
};

export const BIGID_SYNC_TAGS: RecipeField = {
    name: 'sync_tags',
    label: 'Sync Tags',
    tooltip: 'Emit BigID tags as DataHub Tag entities.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.sync_tags',
    rules: null,
    section: 'Advanced',
};

export const BIGID_SYNC_UNLINKED_CLASSIFIERS: RecipeField = {
    name: 'sync_unlinked_classifiers',
    label: 'Sync Unlinked Classifiers',
    tooltip: 'Emit GlossaryTerms for classifier findings that have no Business Glossary linkage in BigID.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.sync_unlinked_classifiers',
    rules: null,
    section: 'Advanced',
};

export const BIGID_SYNC_IDSOR: RecipeField = {
    name: 'sync_idsor',
    label: 'Sync IDSoR Findings',
    tooltip: 'Emit GlossaryTerms for IDSoR (Identity Source of Record) attribute findings.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.sync_idsor',
    rules: null,
    section: 'Advanced',
};

export const BIGID_SYNC_UNSTRUCTURED: RecipeField = {
    name: 'sync_unstructured_enrichment',
    label: 'Enrich Unstructured Sources',
    tooltip: 'Emit dataset-level GlossaryTerms and profiles for unstructured/email sources (SharePoint, Drive, etc.).',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.sync_unstructured_enrichment',
    rules: null,
    section: 'Advanced',
};

export const BIGID_STATEFUL_INGESTION: RecipeField = {
    name: 'stateful_ingestion.enabled',
    label: 'Enable Stateful Ingestion',
    tooltip: 'Enable stateful ingestion to track and remove stale metadata.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.stateful_ingestion.enabled',
    rules: null,
    section: 'Advanced',
};

const allFields: RecipeField[] = [
    BIGID_URL,
    BIGID_USER_TOKEN,
    BIGID_ACCESS_TOKEN,
    BIGID_ENV,
    BIGID_PLATFORM_INSTANCE,
    BIGID_CONNECTION_ALLOW,
    BIGID_CONNECTION_DENY,
    BIGID_MIN_CONFIDENCE,
    BIGID_CREATE_DATASETS,
    BIGID_CONFIDENCE_LEVEL_TAG,
    BIGID_SYNC_TAGS,
    BIGID_SYNC_UNLINKED_CLASSIFIERS,
    BIGID_SYNC_IDSOR,
    BIGID_SYNC_UNSTRUCTURED,
    BIGID_STATEFUL_INGESTION,
];

export default allFields;
