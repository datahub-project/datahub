import i18next from 'i18next';
import { describe, expect, it } from 'vitest';

import { PROFILING_ENABLED, REMOVE_STALE_METADATA_ENABLED } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import {
    GLUE_PROFILING_ENABLED,
    GLUE_REMOVE_STALE_METADATA_ENABLED,
} from '@app/ingestV2/source/builder/RecipeForm/glue';
import {
    ORACLE,
    ORACLE_INCLUDE_QUERY_USAGE,
    ORACLE_PROFILING_ENABLED,
    includeQueryUsageFieldPath,
} from '@app/ingestV2/source/builder/RecipeForm/oracle';

const NAMESPACE = 'ingestion.sourceBuilder';

// Tests load translations synchronously before any module is imported, so a
// label snapshotted at import time already looks right. Changing the
// translation afterwards is what tells a lazy label from a snapshot.
function withTranslation(key: string, value: string, run: () => void) {
    const original = i18next.t(`${NAMESPACE}:${key}`);
    i18next.addResourceBundle('en', NAMESPACE, { [key]: value }, true, true);
    try {
        run();
    } finally {
        i18next.addResourceBundle('en', NAMESPACE, { [key]: original }, true, true);
    }
}

describe('ORACLE_INCLUDE_QUERY_USAGE', () => {
    const readValue = (config: Record<string, unknown>) =>
        ORACLE_INCLUDE_QUERY_USAGE.getValueFromRecipeOverride?.({ source: { config } });

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

    it('drops the Looker key written by the old form', () => {
        const recipe = { source: { config: { extract_usage_history: true, host_port: 'db:1521' } } };

        const updated = ORACLE_INCLUDE_QUERY_USAGE.setValueOnRecipeOverride?.(recipe, true);

        expect(updated.source.config.extract_usage_history).toBeUndefined();
        expect(updated.source.config.include_query_usage).toBe(true);
        expect(updated.source.config.include_usage_stats).toBe(true);
        expect(updated.source.config.host_port).toBe('db:1521');
    });

    it('shows a recipe saved by the old form as checked', () => {
        expect(readValue({ extract_usage_history: true })).toBe(true);
        expect(readValue({ extract_usage_history: false })).toBe(false);
    });

    it('prefers the valid flags over the legacy key when both are present', () => {
        expect(readValue({ extract_usage_history: false, include_query_usage: true, include_usage_stats: true })).toBe(
            true,
        );
        expect(readValue({ extract_usage_history: true, include_query_usage: false })).toBe(false);
    });

    it('is checked only when both flags are set', () => {
        expect(readValue({ include_query_usage: true, include_usage_stats: true })).toBe(true);
        expect(readValue({ include_query_usage: true })).toBe(false);
        expect(readValue({ include_usage_stats: true })).toBe(false);
        expect(readValue({})).toBe(false);
    });

    it('is the only usage toggle in the Oracle form', () => {
        const fieldPaths = RECIPE_FIELDS[ORACLE].advancedFields.map((field) => field.fieldPath);

        expect(fieldPaths).toContain(includeQueryUsageFieldPath);
        // extract_usage_history is a Looker option; OracleConfig rejects it as an extra field.
        expect(fieldPaths).not.toContain('source.config.extract_usage_history');
    });
});

describe('fields derived from shared i18n fields', () => {
    it('resolve the label at read time, not at import time', () => {
        withTranslation('fields.tableProfiling', 'Profiling (changed)', () => {
            expect(ORACLE_PROFILING_ENABLED.label).toBe('Profiling (changed)');
            expect(GLUE_PROFILING_ENABLED.label).toBe('Profiling (changed)');
        });
        withTranslation('fields.removeStaleMetadata', 'Stale metadata (changed)', () => {
            expect(GLUE_REMOVE_STALE_METADATA_ENABLED.label).toBe('Stale metadata (changed)');
        });
        expect(ORACLE_PROFILING_ENABLED.label).toBe(PROFILING_ENABLED.label);
        expect(GLUE_REMOVE_STALE_METADATA_ENABLED.label).toBe(REMOVE_STALE_METADATA_ENABLED.label);
    });

    it('keep the shared field wiring', () => {
        expect(ORACLE_PROFILING_ENABLED.name).toBe(PROFILING_ENABLED.name);
        expect(ORACLE_PROFILING_ENABLED.fieldPath).toBe(PROFILING_ENABLED.fieldPath);
        expect(ORACLE_PROFILING_ENABLED.setValueOnRecipeOverride).toBe(PROFILING_ENABLED.setValueOnRecipeOverride);
        expect(GLUE_REMOVE_STALE_METADATA_ENABLED.setValueOnRecipeOverride).toBe(
            REMOVE_STALE_METADATA_ENABLED.setValueOnRecipeOverride,
        );
    });
});
