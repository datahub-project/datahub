import { describe, expect, it } from 'vitest';

import {
    getColumnProfilingCheckboxValue,
    profilingEnabledFieldPath,
    profilingTableLevelOnlyFieldPath,
    removeStaleMetadataEnabledFieldPath,
    setFieldValueOnRecipe,
    setListValuesOnRecipe,
    setRemoveStaleMetadataOnRecipe,
    statefulIngestionEnabledFieldPath,
    updateProfilingFields,
} from '@app/ingestV2/source/builder/RecipeForm/common';

describe('setFieldValueOnRecipe', () => {
    const accountIdFieldPath = 'source.config.account_id';
    const testProfilingEnabledFieldPath = 'source.config.profiling.enabled';
    const dottedFieldPath = ['source', 'config', 'profiling', 'enabled.test'];

    it('should set the field value on a recipe object when it was not defined', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'test', accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { account_id: 'test' } } });
    });

    it('should update the field value on a recipe object when it was defined', () => {
        const recipe = { source: { config: { account_id: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'edited!', accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { account_id: 'edited!' } } });
    });

    it('should update the field value on a recipe without changing any other fields', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'edited!', accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', account_id: 'edited!' } } });
    });

    it('should clear the key: value pair when passing in null', () => {
        const recipe = { source: { config: { existingField: true, account_id: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { existingField: true } } });
    });

    it('should return the original recipe when passing in undefined', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, undefined, accountIdFieldPath);
        expect(updatedRecipe).toMatchObject(recipe);
    });

    it('should set the field value on a recipe object when it was not defined and has a parent', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, true, testProfilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { profiling: { enabled: true } } } });
    });

    it('should update the field value on a recipe object when it was defined and has a parent', () => {
        const recipe = { source: { config: { profiling: { enabled: true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, false, testProfilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { profiling: { enabled: false } } } });
    });

    it('should update the field value with a parent on a recipe without changing any other fields', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, false, testProfilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', profiling: { enabled: false } } } });
    });

    it('should clear the field and its parent when passing in null and field is only child of parent', () => {
        const recipe = { source: { config: { test: 'test', profiling: { enabled: true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, testProfilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test' } } });
    });

    it('should clear the field but not its parent when passing in null and parent has other children', () => {
        const recipe = { source: { config: { test: 'test', profiling: { enabled: true, testing: 'hello' } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, 'source.config.profiling.testing');
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', profiling: { enabled: true } } } });
    });

    it('should set the field when the key is a dotted value', () => {
        const recipe = { source: { config: { test: 'test', profiling: { 'enabled.test': true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, false, dottedFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { test: 'test', profiling: { 'enabled.test': false } } },
        });
    });

    it('should clear the dotted field and its parent when passing in null and field is only child of parent', () => {
        const recipe = { source: { config: { test: 'test', profiling: { 'enabled.test': true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, dottedFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { test: 'test' } },
        });
    });

    it('should clear the dotted field but not its parent when passing in null and parent has other children', () => {
        const recipe = { source: { config: { test: 'test', profiling: { 'enabled.test': true, testing: 'this' } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, dottedFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', profiling: { testing: 'this' } } } });
    });
});

describe('setListValuesOnRecipe', () => {
    const tableAllowFieldPath = 'source.config.table_pattern.allow';

    it('should update list values on a recipe when it was not defined', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { table_pattern: { allow: ['*test_pattern'] } } } });
    });

    it('should update list values on a recipe when it was defined', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern(edit)'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern(edit)'] } } },
        });
    });

    it('should append list values on a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern', 'new'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern', 'new'] } } },
        });
    });

    it('should remove list values on a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern', 'remove_me'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern'] } } },
        });
    });

    it('should remove empty values from the list when updating a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern', '', '', ''], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern'] } } },
        });
    });

    it('should clear the value and its parent when passing in empty list and parent has no other children', () => {
        const recipe = { source: { config: { existingField: true, table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, [], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { existingField: true } } });
    });

    it('should clear the value but not its parent when passing in empty list and parent has other children', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'], deny: ['test_deny'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, [], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { table_pattern: { deny: ['test_deny'] } } } });
    });
});

describe('updateProfilingFields', () => {
    it('should enable table profiling only when table profiling is enabled and column profiling is disabled', () => {
        const recipe = { source: { config: {} } };
        const result = updateProfilingFields(recipe, true, false);

        expect(result.source.config.profiling.enabled).toBe(true);
        expect(result.source.config.profiling.profile_table_level_only).toBe(true);
    });

    it('should enable both table and column profiling when both are enabled', () => {
        const recipe = { source: { config: {} } };
        const result = updateProfilingFields(recipe, true, true);

        expect(result.source.config.profiling.enabled).toBe(true);
        expect(result.source.config.profiling.profile_table_level_only).toBe(false);
    });

    it('should disable profiling when table profiling is disabled', () => {
        const recipe = { source: { config: {} } };
        const result = updateProfilingFields(recipe, false, false);

        expect(result.source.config.profiling.enabled).toBe(false);
        expect(result.source.config.profiling.profile_table_level_only).toBeUndefined();
    });

    it('should disable profiling when table profiling is disabled regardless of column profiling', () => {
        const recipe = { source: { config: {} } };
        const result = updateProfilingFields(recipe, false, true);

        expect(result.source.config.profiling.enabled).toBe(false);
        expect(result.source.config.profiling.profile_table_level_only).toBeUndefined();
    });
});

describe('getColumnProfilingCheckboxValue', () => {
    it('should return true when profile_table_level_only is false', () => {
        const recipe = {
            source: {
                config: {
                    profiling: {
                        profile_table_level_only: false,
                    },
                },
            },
        };
        const result = getColumnProfilingCheckboxValue(recipe);

        expect(result).toBe(true);
    });

    it('should return false when profile_table_level_only is true', () => {
        const recipe = {
            source: {
                config: {
                    profiling: {
                        profile_table_level_only: true,
                    },
                },
            },
        };
        const result = getColumnProfilingCheckboxValue(recipe);

        expect(result).toBe(false);
    });

    it('should return false when profile_table_level_only is undefined', () => {
        const recipe = { source: { config: {} } };
        const result = getColumnProfilingCheckboxValue(recipe);

        expect(result).toBe(false);
    });
});

describe('setRemoveStaleMetadataOnRecipe', () => {
    it('should enable both stateful ingestion and remove_stale_metadata when value is true', () => {
        const recipe = { source: { config: {} } };
        const result = setRemoveStaleMetadataOnRecipe(recipe, true);

        expect(result.source.config.stateful_ingestion.enabled).toBe(true);
        expect(result.source.config.stateful_ingestion.remove_stale_metadata).toBe(true);
    });

    it('should disable stateful ingestion and omit remove_stale_metadata when value is false', () => {
        const recipe = {
            source: {
                config: {
                    stateful_ingestion: {
                        enabled: true,
                        remove_stale_metadata: true,
                    },
                },
            },
        };
        const result = setRemoveStaleMetadataOnRecipe(recipe, false);

        expect(result.source.config.stateful_ingestion.enabled).toBe(false);
        expect(result.source.config.stateful_ingestion.remove_stale_metadata).toBeUndefined();
    });
});

describe('Field path constants', () => {
    it('should have correct field paths', () => {
        expect(profilingEnabledFieldPath).toBe('source.config.profiling.enabled');
        expect(profilingTableLevelOnlyFieldPath).toBe('source.config.profiling.profile_table_level_only');
        expect(statefulIngestionEnabledFieldPath).toBe('source.config.stateful_ingestion.enabled');
        expect(removeStaleMetadataEnabledFieldPath).toBe('source.config.stateful_ingestion.remove_stale_metadata');
    });
});
