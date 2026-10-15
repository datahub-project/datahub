import { describe, expect, it } from 'vitest';

import { PROFILING_ENABLED } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { GLUE_PROFILING_ENABLED } from '@app/ingestV2/source/builder/RecipeForm/glue';
import {
    ORACLE,
    ORACLE_INCLUDE_QUERY_USAGE,
    ORACLE_PROFILING_ENABLED,
    includeQueryUsageFieldPath,
    includeUsageStatsFieldPath,
} from '@app/ingestV2/source/builder/RecipeForm/oracle';

describe('ORACLE_INCLUDE_QUERY_USAGE', () => {
    it('writes both flags the Oracle source needs for V$SQL usage', () => {
        const recipe = { source: { type: 'oracle', config: { host_port: 'db:1521' } } };

        const updated = ORACLE_INCLUDE_QUERY_USAGE.setValueOnRecipeOverride?.(recipe, true);

        expect(updated.source.config.include_query_usage).toBe(true);
        expect(updated.source.config.include_usage_stats).toBe(true);
        expect(updated.source.config.host_port).toBe('db:1521');
    });

    it('clears both flags when unchecked', () => {
        const recipe = { source: { config: { include_query_usage: true, include_usage_stats: true } } };

        const updated = ORACLE_INCLUDE_QUERY_USAGE.setValueOnRecipeOverride?.(recipe, false);

        expect(updated.source.config.include_query_usage).toBe(false);
        expect(updated.source.config.include_usage_stats).toBe(false);
    });

    it('reads its value from include_query_usage', () => {
        expect(ORACLE_INCLUDE_QUERY_USAGE.fieldPath).toBe(includeQueryUsageFieldPath);
        expect(includeUsageStatsFieldPath).toBe('source.config.include_usage_stats');
    });

    it('is the only usage toggle in the Oracle form', () => {
        const fieldPaths = RECIPE_FIELDS[ORACLE].advancedFields.map((field) => field.fieldPath);

        expect(fieldPaths).toContain(includeQueryUsageFieldPath);
        // extract_usage_history is a Looker option; OracleConfig rejects it as an extra field.
        expect(fieldPaths).not.toContain('source.config.extract_usage_history');
    });
});

describe('profiling fields derived from PROFILING_ENABLED', () => {
    it('resolve the shared label lazily', () => {
        expect(PROFILING_ENABLED.label).toBeTruthy();
        expect(ORACLE_PROFILING_ENABLED.label).toBe(PROFILING_ENABLED.label);
        expect(GLUE_PROFILING_ENABLED.label).toBe(PROFILING_ENABLED.label);
    });

    it('keep the shared field wiring', () => {
        expect(ORACLE_PROFILING_ENABLED.name).toBe(PROFILING_ENABLED.name);
        expect(ORACLE_PROFILING_ENABLED.fieldPath).toBe(PROFILING_ENABLED.fieldPath);
        expect(ORACLE_PROFILING_ENABLED.setValueOnRecipeOverride).toBe(PROFILING_ENABLED.setValueOnRecipeOverride);
    });
});
