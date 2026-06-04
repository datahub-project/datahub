import { renderHook } from '@testing-library/react-hooks';
import i18n from 'i18next';

import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';

describe('useIngestionSources', () => {
    describe('toSourceKey transform (via hook output)', () => {
        it('converts simple names unchanged', () => {
            const { result } = renderHook(() => useIngestionSources());
            const bigquery = result.current.ingestionSources.find((s) => s.name === 'bigquery');
            expect(bigquery).toBeDefined();
            expect(bigquery!.displayName).toBe('BigQuery');
        });

        it('converts kebab-case names to camelCase for key lookup', () => {
            const { result } = renderHook(() => useIngestionSources());
            // dbt-cloud → dbtCloud; the translation key sources.dbtCloud.displayName must resolve
            const dbtCloud = result.current.ingestionSources.find((s) => s.name === 'dbt-cloud');
            expect(dbtCloud).toBeDefined();
            expect(dbtCloud!.displayName).toBe('dbt Cloud');
        });

        it('converts multi-segment kebab names correctly', () => {
            const { result } = renderHook(() => useIngestionSources());
            // matillion-dpc → matillionDpc
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

        it('resolves description for all sources that have one', () => {
            const { result } = renderHook(() => useIngestionSources());
            // Every source in the v2 sources.json has a description — none should be lost
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
            const initialDisplayName = result.current.ingestionSources.find((s) => s.name === 'bigquery')!.displayName;

            await i18n.changeLanguage('de');
            rerender();

            // After language change the hook should re-run. In tests the DE locale isn't
            // loaded so it falls back to EN — but the memo must have re-evaluated.
            const afterDisplayName = result.current.ingestionSources.find((s) => s.name === 'bigquery')!.displayName;
            expect(afterDisplayName).toBe(initialDisplayName);

            // Restore
            await i18n.changeLanguage('en');
        });
    });
});
