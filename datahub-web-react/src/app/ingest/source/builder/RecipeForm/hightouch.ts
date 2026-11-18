import i18next from 'i18next';

import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const HIGHTOUCH_API_KEY: RecipeField = {
    name: 'api_key',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.apiKey.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.apiKey.tooltip');
    },
    type: FieldType.SECRET,
    fieldPath: 'source.config.api_config.api_key',
    placeholder: 'your_api_key_here',
    rules: null,
    required: true,
};

export const HIGHTOUCH_EMIT_MODELS: RecipeField = {
    name: 'emit_models_as_datasets',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.emitModels.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.emitModels.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.emit_models_as_datasets',
    rules: null,
};

export const HIGHTOUCH_INCLUDE_SYNC_RUNS: RecipeField = {
    name: 'include_sync_runs',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.includeSyncRuns.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.includeSyncRuns.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_sync_runs',
    rules: null,
};

export const HIGHTOUCH_MAX_SYNC_RUNS: RecipeField = {
    name: 'max_sync_runs_per_sync',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.maxSyncRuns.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.maxSyncRuns.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.max_sync_runs_per_sync',
    placeholder: '10',
    get rules() {
        return [{ pattern: /^[1-9]\d*$/, message: i18next.t('ingestion.sourceBuilder:hightouch.maxSyncRuns.invalid') }];
    },
};

export const HIGHTOUCH_SYNC_ALLOW: RecipeField = {
    name: 'sync_patterns.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.syncAllow.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.syncAllow.tooltip');
    },
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: 'source.config.sync_patterns.allow',
    rules: null,
    get section() {
        return i18next.t('ingestion.sourceBuilder:hightouch.syncs.section');
    },
    placeholder: '.*',
};

export const HIGHTOUCH_SYNC_DENY: RecipeField = {
    name: 'sync_patterns.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.syncDeny.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.syncDeny.tooltip');
    },
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: 'source.config.sync_patterns.deny',
    rules: null,
    get section() {
        return i18next.t('ingestion.sourceBuilder:hightouch.syncs.section');
    },
    placeholder: 'test-.*',
};

export const HIGHTOUCH_MODEL_ALLOW: RecipeField = {
    name: 'model_patterns.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.modelAllow.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.modelAllow.tooltip');
    },
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: 'source.config.model_patterns.allow',
    rules: null,
    get section() {
        return i18next.t('ingestion.sourceBuilder:hightouch.models.section');
    },
    placeholder: '.*',
};

export const HIGHTOUCH_MODEL_DENY: RecipeField = {
    name: 'model_patterns.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:hightouch.modelDeny.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:hightouch.modelDeny.tooltip');
    },
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: 'source.config.model_patterns.deny',
    rules: null,
    get section() {
        return i18next.t('ingestion.sourceBuilder:hightouch.models.section');
    },
    placeholder: 'draft-.*',
};
