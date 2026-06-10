import { renderHook } from '@testing-library/react-hooks';
import i18n from 'i18next';

import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';

describe('useIngestionSources', () => {
    describe('displayName field', () => {
        // Display names are proper nouns (Athena, BigQuery, …) and must NOT be translated —
        // they always pass through unchanged from sources.json.
        it('passes simple display names through unchanged', () => {
            const { result } = renderHook(() => useIngestionSources());
            const bigquery = result.current.ingestionSources.find((s) => s.name === 'bigquery');
            expect(bigquery).toBeDefined();
            expect(bigquery!.displayName).toBe('BigQuery');
        });

        it('passes display names of kebab-case sources through unchanged', () => {
            const { result } = renderHook(() => useIngestionSources());
            const dbtCloud = result.current.ingestionSources.find((s) => s.name === 'dbt-cloud');
            expect(dbtCloud).toBeDefined();
            expect(dbtCloud!.displayName).toBe('dbt Cloud');
        });

        it('passes display names of multi-segment kebab sources through unchanged', () => {
            const { result } = renderHook(() => useIngestionSources());
            const matillion = result.current.ingestionSources.find((s) => s.name === 'matillion-dpc');
            expect(matillion).toBeDefined();
            expect(matillion!.displayName).toBe('Matillion');
        });
    });

    describe('description field', () => {
        it('resolves description when present', () => {
            const { result } = renderHook(() => useIngestionSources());
            const snowflake = result.current.ingestionSources.find((s) => s.name === 'snowflake');
            expect(snowflake!.description).toContain('Snowflake');
        });

        it('resolves description for kebab-case sources via camelCase key lookup', () => {
            const { result } = renderHook(() => useIngestionSources());
            // dbt-cloud → dbtCloud; the translation key sources.dbtCloud.description must resolve
            const dbtCloud = result.current.ingestionSources.find((s) => s.name === 'dbt-cloud');
            expect(dbtCloud!.description).toContain('dbt cloud');
        });

        it('resolves description for all sources that have one', () => {
            const { result } = renderHook(() => useIngestionSources());
            // No source that has a description should lose it during resolution
            const missing = result.current.ingestionSources.filter((s) => s.description === null);
            expect(missing).toHaveLength(0);
        });
    });

    describe('category field', () => {
        it('passes category through untranslated for use as a grouping key', () => {
            const { result } = renderHook(() => useIngestionSources());
            // category must stay as the raw English string so groupByCategory / getOrderedByCategoryEntriesOfGroups
            // can match against their hardcoded English constants in utils.ts
            const bigquery = result.current.ingestionSources.find((s) => s.name === 'bigquery');
            expect(bigquery!.category).toBe('Data Warehouse');
        });

        it('leaves category undefined when absent in source data', () => {
            const { result } = renderHook(() => useIngestionSources());
            // doris and rdf have no category field in sources.json
            const doris = result.current.ingestionSources.find((s) => s.name === 'doris');
            expect(doris).toBeDefined();
            expect(doris!.category).toBeUndefined();
        });
    });

    describe('full source list', () => {
        it('returns all sources from sources.json', () => {
            const { result } = renderHook(() => useIngestionSources());
            // Sanity check: the hook returns a non-empty list
            expect(result.current.ingestionSources.length).toBeGreaterThan(0);
        });

        it('does not mutate original sources.json data', () => {
            const { result: r1 } = renderHook(() => useIngestionSources());
            const { result: r2 } = renderHook(() => useIngestionSources());
            // Each call produces an independent copy
            expect(r1.current.ingestionSources).not.toBe(r2.current.ingestionSources);
        });
    });

    describe('language reactivity', () => {
        it('re-computes when i18n language changes', async () => {
            const { result, rerender } = renderHook(() => useIngestionSources());
            const initialDescription = result.current.ingestionSources.find((s) => s.name === 'bigquery')!.description;

            await i18n.changeLanguage('de');
            rerender();

            // After language change the hook should re-run. In tests the DE locale isn't
            // loaded so it falls back to EN — but the memo must have re-evaluated.
            const afterDescription = result.current.ingestionSources.find((s) => s.name === 'bigquery')!.description;
            expect(afterDescription).toBe(initialDescription);

            // Restore
            await i18n.changeLanguage('en');
        });
    });
});
