import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const HIGHTOUCH_API_KEY: RecipeField = {
    name: 'api_key',
    label: 'API Key',
    tooltip: 'Your Hightouch API Key. Generate one from Settings → API Keys in your Hightouch workspace.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.api_config.api_key',
    placeholder: 'your_api_key_here',
    rules: null,
    required: true,
};

export const HIGHTOUCH_EMIT_MODELS: RecipeField = {
    name: 'emit_models_as_datasets',
    label: 'Emit Models as Datasets',
    tooltip:
        'Whether to emit Hightouch models as Dataset entities. When enabled, models appear as separate datasets with platform "hightouch".',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.emit_models_as_datasets',
    rules: null,
};

export const HIGHTOUCH_INCLUDE_COLUMN_LINEAGE: RecipeField = {
    name: 'include_column_lineage',
    label: 'Include Column Lineage',
    tooltip: 'Extract field mappings from sync configurations to create column-to-column lineage.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_column_lineage',
    rules: null,
};

export const HIGHTOUCH_INCLUDE_SYNC_RUNS: RecipeField = {
    name: 'include_sync_runs',
    label: 'Include Sync Runs',
    tooltip: 'Ingest sync execution history as DataProcessInstances with detailed statistics.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_sync_runs',
    rules: null,
};

export const HIGHTOUCH_MAX_SYNC_RUNS: RecipeField = {
    name: 'max_sync_runs_per_sync',
    label: 'Max Sync Runs per Sync',
    tooltip: 'Maximum number of historical sync runs to ingest per sync. Default is 10.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.max_sync_runs_per_sync',
    placeholder: '10',
    rules: null,
};

export const HIGHTOUCH_SYNC_ALLOW: RecipeField = {
    name: 'sync_patterns.allow',
    label: 'Sync Allow Patterns',
    tooltip: 'Only include syncs that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.sync_patterns.allow',
    rules: null,
    section: 'Syncs',
    placeholder: '.*',
};

export const HIGHTOUCH_SYNC_DENY: RecipeField = {
    name: 'sync_patterns.deny',
    label: 'Sync Deny Patterns',
    tooltip: 'Exclude syncs that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.sync_patterns.deny',
    rules: null,
    section: 'Syncs',
    placeholder: 'test-.*',
};

export const HIGHTOUCH_MODEL_ALLOW: RecipeField = {
    name: 'model_patterns.allow',
    label: 'Model Allow Patterns',
    tooltip: 'Only include models that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.model_patterns.allow',
    rules: null,
    section: 'Models',
    placeholder: '.*',
};

export const HIGHTOUCH_MODEL_DENY: RecipeField = {
    name: 'model_patterns.deny',
    label: 'Model Deny Patterns',
    tooltip: 'Exclude models that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.model_patterns.deny',
    rules: null,
    section: 'Models',
    placeholder: 'draft-.*',
};
