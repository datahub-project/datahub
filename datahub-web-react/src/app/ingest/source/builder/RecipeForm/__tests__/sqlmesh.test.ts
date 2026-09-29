import { describe, expect, it } from 'vitest';

import { SQLMESH_REMOVE_STALE_METADATA } from '@app/ingest/source/builder/RecipeForm/sqlmesh';

describe('SQLMesh (legacy ingest) Remove Stale Metadata toggle', () => {
    describe('getValueFromRecipeOverride', () => {
        it('is unchecked for a new (empty) recipe', () => {
            const recipe = { source: { config: {} } };
            expect(SQLMESH_REMOVE_STALE_METADATA.getValueFromRecipeOverride?.(recipe)).toBe(false);
        });

        it('reflects remove_stale_metadata, not stateful_ingestion.enabled', () => {
            // Stateful ingestion on but stale-delete explicitly off must render unchecked
            // (the previous enabled-backed read showed the opposite of the real setting).
            const recipe = {
                source: { config: { stateful_ingestion: { enabled: true, remove_stale_metadata: false } } },
            };
            expect(SQLMESH_REMOVE_STALE_METADATA.getValueFromRecipeOverride?.(recipe)).toBe(false);
        });

        it('is checked when remove_stale_metadata is true', () => {
            const recipe = {
                source: { config: { stateful_ingestion: { enabled: true, remove_stale_metadata: true } } },
            };
            expect(SQLMESH_REMOVE_STALE_METADATA.getValueFromRecipeOverride?.(recipe)).toBe(true);
        });

        it('is checked when stateful ingestion is on and remove_stale_metadata is omitted', () => {
            // The backend defaults remove_stale_metadata to true, so the compact
            // recipe (enabled: true, key omitted) is really soft-deleting stale
            // entities — the toggle must not conceal that by rendering unchecked.
            const recipe = {
                source: { config: { stateful_ingestion: { enabled: true } } },
            };
            expect(SQLMESH_REMOVE_STALE_METADATA.getValueFromRecipeOverride?.(recipe)).toBe(true);
        });
    });

    describe('setValueOnRecipeOverride', () => {
        it('turns both enabled and remove_stale_metadata on together', () => {
            const recipe = { source: { config: {} } };
            const result = SQLMESH_REMOVE_STALE_METADATA.setValueOnRecipeOverride?.(recipe, true);

            expect(result.source.config.stateful_ingestion.enabled).toBe(true);
            expect(result.source.config.stateful_ingestion.remove_stale_metadata).toBe(true);
        });

        it('drops remove_stale_metadata and disables stateful ingestion when turned off', () => {
            const recipe = {
                source: { config: { stateful_ingestion: { enabled: true, remove_stale_metadata: true } } },
            };
            const result = SQLMESH_REMOVE_STALE_METADATA.setValueOnRecipeOverride?.(recipe, false);

            expect(result.source.config.stateful_ingestion.enabled).toBe(false);
            expect(result.source.config.stateful_ingestion.remove_stale_metadata).toBeUndefined();
        });
    });
});
