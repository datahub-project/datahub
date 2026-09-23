import { describe, expect, it } from 'vitest';

import { INCLUDE_SEEDS, INCLUDE_SOURCES } from '@app/ingestV2/source/builder/RecipeForm/dbt_cloud';

describe('dbt_cloud INCLUDE_SEEDS', () => {
    it('writes the seeds value to entities_enabled.seeds, not sources', () => {
        // Regression: a copy-paste in setValueOnRecipeOverride used to write to
        // entities_enabled.sources, silently overwriting the Sources toggle and
        // never persisting the Seeds value.
        const recipe = {
            source: {
                config: {
                    entities_enabled: { sources: 'YES' },
                },
            },
        };

        const result = INCLUDE_SEEDS.setValueOnRecipeOverride?.(recipe, false);

        expect(result.source.config.entities_enabled.seeds).toBe('NO');
        expect(result.source.config.entities_enabled.sources).toBe('YES');
    });

    it('writes YES when the user enables seeds', () => {
        const recipe = { source: { config: {} } };
        const result = INCLUDE_SEEDS.setValueOnRecipeOverride?.(recipe, true);
        expect(result.source.config.entities_enabled.seeds).toBe('YES');
    });
});

describe('dbt_cloud INCLUDE_SOURCES', () => {
    it('writes the sources value to entities_enabled.sources independent of seeds', () => {
        const recipe = {
            source: { config: { entities_enabled: { seeds: 'YES' } } },
        };
        const result = INCLUDE_SOURCES.setValueOnRecipeOverride?.(recipe, false);
        expect(result.source.config.entities_enabled.sources).toBe('NO');
        expect(result.source.config.entities_enabled.seeds).toBe('YES');
    });
});
