import { describe, expect, it } from 'vitest';

import { SourceConfig } from '@app/ingestV2/source/builder/types';
import {
    computeRows,
    getPillLabel,
    groupByCategory,
    sortByPopularFirst,
} from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils';

const base = {
    urn: 'url:li:dataPlatform:platform1',
    displayName: 'name',
    docsUrl: 'https://docs.datahub.com',
    recipe: '',
};

const make = (partial: Partial<SourceConfig>): SourceConfig => ({
    ...base,
    name: partial.name || '',
    category: partial.category,
    isPopular: partial.isPopular,
    isExternal: partial.isExternal,
});

describe('utils', () => {
    describe('groupByCategory', () => {
        it('should group sources by category', () => {
            const sources: SourceConfig[] = [
                make({ name: 'A', category: 'DB' }),
                make({ name: 'B', category: 'API' }),
                make({ name: 'C', category: 'DB' }),
            ];

            const result = groupByCategory(sources);

            expect(result).toEqual({
                DB: [sources[0], sources[2]],
                API: [sources[1]],
            });
        });

        it('should place items without category under "Other"', () => {
            const sources: SourceConfig[] = [
                make({ name: 'A' }),
                make({ name: 'B', category: '' }),
                make({ name: 'C', category: undefined }),
            ];

            const result = groupByCategory(sources);

            expect(result.Other.length).toBe(3);
        });

        it('should return empty object when no sources provided', () => {
            expect(groupByCategory([])).toEqual({});
        });
    });

    describe('sortByPopularFirst', () => {
        it('should place popular sources first', () => {
            const sources: SourceConfig[] = [
                make({ name: 'A', isPopular: false }),
                make({ name: 'B', isPopular: true }),
                make({ name: 'C', isPopular: false }),
                make({ name: 'D', isPopular: true }),
            ];

            const sorted = sortByPopularFirst(sources);

            expect(sorted.map((s) => s.name)).toEqual(['B', 'D', 'A', 'C']);
        });

        it('should handle array with no popular sources', () => {
            const sources = [make({ name: 'A' }), make({ name: 'B' })];

            expect(sortByPopularFirst(sources)).toEqual(sources);
        });

        it('should handle empty array', () => {
            expect(sortByPopularFirst([])).toEqual([]);
        });
    });

    describe('computeRows', () => {
        const p = (name: string) => make({ name, isPopular: true });
        const np = (name: string) => make({ name, isPopular: false });

        it('should always use at least 1 column', () => {
            const { visible, hidden } = computeRows([p('A')], [np('1')], 0);
            expect(visible.length).toBe(2);
            expect(hidden.length).toBe(0);
        });

        it('should show only popular when no nonPopular provided', () => {
            const { visible, hidden } = computeRows([p('A'), p('B')], [], 3);
            expect(visible.map((v) => v.name)).toEqual(['A', 'B']);
            expect(hidden).toEqual([]);
        });

        it('should allocate slots correctly when last popular row has no free slots', () => {
            const popular = [p('A'), p('B')];
            const nonPopular = [np('1'), np('2'), np('3')];

            const { visible, hidden } = computeRows(popular, nonPopular, 2);

            expect(visible.map((v) => v.name)).toEqual(['A', 'B', '1']);
            expect(hidden.map((h) => h.name)).toEqual(['2', '3']);
        });

        it('should use free slots of last row before starting new one', () => {
            const popular = [p('A')];
            const nonPopular = [np('1'), np('2'), np('3')];

            const { visible, hidden } = computeRows(popular, nonPopular, 3);

            expect(visible.map((v) => v.name)).toEqual(['A', '1']);
            expect(hidden.map((h) => h.name)).toEqual(['2', '3']);
        });

        it('should move hidden item to visible when only one is hidden', () => {
            const popular = [p('A')];
            const nonPopular = [np('1'), np('2')];

            const { visible, hidden } = computeRows(popular, nonPopular, 3);

            expect(visible.map((v) => v.name)).toEqual(['A', '1', '2']);
            expect(hidden.length).toBe(0);
        });

        it('should return all nonPopular as hidden when only 1 column', () => {
            const { visible, hidden } = computeRows([p('A')], [np('1'), np('2'), np('3')], 1);

            expect(visible.map((v) => v.name)).toEqual(['A']);
            expect(hidden.map((h) => h.name)).toEqual(['1', '2', '3']);
        });

        it('should handle empty popular list', () => {
            const { visible, hidden } = computeRows([], [np('1'), np('2'), np('3')], 3);

            expect(visible.map((v) => v.name)).toEqual(['1', '2', '3']);
            expect(hidden.map((h) => h.name)).toEqual([]);
        });
    });

    describe('getPillLabel', () => {
        it('should return "Popular" when source is popular', () => {
            expect(getPillLabel(make({ name: 'A', isPopular: true }))).toBe('Popular');
        });

        it('should return "External" when source is external', () => {
            expect(getPillLabel(make({ name: 'A', isExternal: true }))).toBe('External');
        });

        it('should return empty string for normal sources', () => {
            expect(getPillLabel(make({ name: 'A' }))).toBe('');
        });
    });
});
