import { describe, expect, it } from 'vitest';

import sources from '@app/ingestV2/source/builder/sources.json';
import { CATEGORY_I18N_KEYS } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils';
import enSources from '@src/i18n/locales/en/ingest.sources.json';

const countByCategory = sources.reduce<Record<string, number>>((acc, source) => {
    const category = source.category ?? '(none)';
    acc[category] = (acc[category] ?? 0) + 1;
    return acc;
}, {});

describe('ingestion source categories', () => {
    it('gives every tile a category that has a translation key', () => {
        const unknown = sources
            .filter((source) => !source.category || !(source.category in CATEGORY_I18N_KEYS))
            .map((source) => `${source.name}: ${source.category ?? '(none)'}`);

        expect(unknown).toEqual([]);
    });

    it('has no category with a single tile', () => {
        const singles = Object.entries(countByCategory)
            .filter(([, count]) => count < 2)
            .map(([category]) => category);

        expect(singles).toEqual([]);
    });

    it('only lists categories that tiles use, each with an English label', () => {
        const problems = Object.entries(CATEGORY_I18N_KEYS)
            .filter(([category, key]) => !countByCategory[category] || !(key in enSources))
            .map(([category, key]) => `${category} (${key})`);

        expect(problems).toEqual([]);
    });
});
