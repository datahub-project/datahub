/**
 * BigID Recipe Form Fields (V2). Plain fields are defined once in the V1 form
 * (`ingest/source/builder/RecipeForm/bigid.ts`) and re-exported here; V2 only redefines the
 * connection allow/deny fields as FilterRecipeField (a type V1's `common` does not provide).
 *
 * datasource_platform_mapping has no form field — set it via the YAML editor.
 */
import {
    BIGID_ACCESS_TOKEN,
    BIGID_CONFIDENCE_LEVEL_TAG,
    BIGID_CREATE_DATASETS,
    BIGID_ENV,
    BIGID_MIN_CONFIDENCE,
    BIGID_PLATFORM_INSTANCE,
    BIGID_STATEFUL_INGESTION,
    BIGID_SYNC_IDSOR,
    BIGID_SYNC_TAGS,
    BIGID_SYNC_UNLINKED_CLASSIFIERS,
    BIGID_SYNC_UNSTRUCTURED,
    BIGID_URL,
    BIGID_USER_TOKEN,
} from '@app/ingest/source/builder/RecipeForm/bigid';
import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';
import { FilterRecipeField, FilterRule, setListValuesOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';

export {
    BIGID_ACCESS_TOKEN,
    BIGID_CONFIDENCE_LEVEL_TAG,
    BIGID_CREATE_DATASETS,
    BIGID_ENV,
    BIGID_MIN_CONFIDENCE,
    BIGID_PLATFORM_INSTANCE,
    BIGID_STATEFUL_INGESTION,
    BIGID_SYNC_IDSOR,
    BIGID_SYNC_TAGS,
    BIGID_SYNC_UNLINKED_CLASSIFIERS,
    BIGID_SYNC_UNSTRUCTURED,
    BIGID_URL,
    BIGID_USER_TOKEN,
};

const bigidConnectionAllowFieldPath = 'source.config.connection_pattern.allow';
export const BIGID_CONNECTION_ALLOW: FilterRecipeField = {
    name: 'connection_pattern.allow',
    label: 'Connection Allow Patterns',
    tooltip: 'Only include BigID connections (data sources) whose name matches these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: bigidConnectionAllowFieldPath,
    rules: null,
    section: 'Connections',
    rule: FilterRule.INCLUDE,
    filteringResource: 'Connection',
    placeholder: '.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, bigidConnectionAllowFieldPath),
};

const bigidConnectionDenyFieldPath = 'source.config.connection_pattern.deny';
export const BIGID_CONNECTION_DENY: FilterRecipeField = {
    name: 'connection_pattern.deny',
    label: 'Connection Deny Patterns',
    tooltip: 'Exclude BigID connections (data sources) whose name matches these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: bigidConnectionDenyFieldPath,
    rules: null,
    section: 'Connections',
    rule: FilterRule.EXCLUDE,
    filteringResource: 'Connection',
    placeholder: 'sandbox-.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, bigidConnectionDenyFieldPath),
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
