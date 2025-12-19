import { describe, expect, it } from 'vitest';

import {
    getColumnProfilingCheckboxValue,
    profilingEnabledFieldPath,
    profilingTableLevelOnlyFieldPath,
    removeStaleMetadataEnabledFieldPath,
    setRemoveStaleMetadataOnRecipe,
    statefulIngestionEnabledFieldPath,
    updateProfilingFields,
} from '@app/ingestV2/source/builder/RecipeForm/common';

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
