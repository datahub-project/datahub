import { describe, expect, it } from 'vitest';

import { SourceConfig } from '@app/ingestV2/source/builder/types';
import {
    DATA_LAKE_CATEGORY_NAME,
    MISCELLANEOUS_CATEGORY_NAME,
    computeRows,
    getOrderedByCategoryEntriesOfGroups,
    getPillLabel,
    groupByCategory,
    sortByPopularFirst,
    sortByPriorityAndDisplayName,
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
    priority: partial.priority,
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

    describe('getOrderedByCategoryEntriesOfGroups', () => {
        it('should add presorted start categories when they are defined', () => {
            const groups: Record<string, SourceConfig[]> = {
                Test: [make({ name: 'A', category: 'Test' })],
                DATA_LAKE_CATEGORY_NAME: [make({ name: 'B', category: DATA_LAKE_CATEGORY_NAME })],
            };

            const result = getOrderedByCategoryEntriesOfGroups(groups);

            expect(result[0][0]).toBe(DATA_LAKE_CATEGORY_NAME);
            expect(result[1][0]).toBe('Test');
        });

        it('should add presorted end categories when they are defined', () => {
            const groups: Record<string, SourceConfig[]> = {
                [MISCELLANEOUS_CATEGORY_NAME]: [make({ name: 'A', category: MISCELLANEOUS_CATEGORY_NAME })],
                Test: [make({ name: 'B', category: 'Test' })],
            };

            const result = getOrderedByCategoryEntriesOfGroups(groups);

            expect(result[0][0]).toBe('Test');
            expect(result[1][0]).toBe(MISCELLANEOUS_CATEGORY_NAME);
        });

        it('should order alphabetical categories in between', () => {
            const groups: Record<string, SourceConfig[]> = {
                [MISCELLANEOUS_CATEGORY_NAME]: [make({ name: 'Name', category: MISCELLANEOUS_CATEGORY_NAME })],
                C: [make({ name: 'Name', category: 'C' })],
                B: [make({ name: 'Name', category: 'B' })],
                A: [make({ name: 'Name', category: 'A' })],
                DATA_LAKE_CATEGORY_NAME: [make({ name: 'Name', category: DATA_LAKE_CATEGORY_NAME })],
            };

            const result = getOrderedByCategoryEntriesOfGroups(groups);

            expect(result[0][0]).toBe(DATA_LAKE_CATEGORY_NAME);
            expect(result[1][0]).toBe('A');
            expect(result[2][0]).toBe('B');
            expect(result[3][0]).toBe('C');
            expect(result[4][0]).toBe(MISCELLANEOUS_CATEGORY_NAME);
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

    describe('sortByPriorityAndDisplayName', () => {
        it('returns empty array for empty input', () => {
            expect(sortByPriorityAndDisplayName([])).toEqual([]);
        });

        it('handles single item with priority', () => {
            const input: SourceConfig[] = [make({ displayName: 'Test', priority: 5 })];
            expect(sortByPriorityAndDisplayName(input)).toEqual(input);
        });

        it('handles single item without priority', () => {
            const input = [make({ displayName: 'Test' })];
            expect(sortByPriorityAndDisplayName(input)).toEqual(input);
        });

        it('pins priority items above non-priority items regardless of displayName', () => {
            const input: SourceConfig[] = [
                make({ displayName: 'Zebra', priority: 1 }),
                make({ displayName: 'Apple' }),
                make({ displayName: 'Urgent', priority: 0 }),
                make({ displayName: 'Banana' }),
            ];

            const result = sortByPriorityAndDisplayName(input);

            expect(result[0]).toEqual(make({ displayName: 'Urgent', priority: 0 }));
            expect(result[1]).toEqual(make({ displayName: 'Zebra', priority: 1 }));
            expect(result[2]).toEqual(make({ displayName: 'Apple' }));
            expect(result[3]).toEqual(make({ displayName: 'Banana' }));
        });

        it('sorts priority items by priority (ascending), then by displayName', () => {
            const input: SourceConfig[] = [
                make({ displayName: 'Gamma', priority: 1 }),
                make({ displayName: 'Alpha', priority: 0 }),
                make({ displayName: 'Beta', priority: 1 }),
                make({ displayName: 'Delta', priority: 0 }),
            ];

            const result = sortByPriorityAndDisplayName(input);

            expect(result).toEqual([
                make({ displayName: 'Alpha', priority: 0 }),
                make({ displayName: 'Delta', priority: 0 }),
                make({ displayName: 'Beta', priority: 1 }),
                make({ displayName: 'Gamma', priority: 1 }),
            ]);
        });

        it('sorts non-priority items alphabetically by displayName', () => {
            const input: SourceConfig[] = [
                make({ displayName: 'zebra' }),
                make({ displayName: 'Apple' }),
                make({ displayName: 'banana' }),
            ];

            const result = sortByPriorityAndDisplayName(input);

            expect(result).toEqual([
                make({ displayName: 'Apple' }),
                make({ displayName: 'banana' }),
                make({ displayName: 'zebra' }),
            ]);
        });

        it('does not mutate the original array', () => {
            const original: SourceConfig[] = [
                make({ displayName: 'B', priority: 2 }),
                make({ displayName: 'A', priority: 1 }),
                make({ displayName: 'C' }),
            ];

            const originalCopy = structuredClone(original);
            sortByPriorityAndDisplayName(original);

            expect(original).toEqual(originalCopy);
        });

        it('handles all items with priority', () => {
            const input: SourceConfig[] = [
                make({ displayName: 'Last', priority: 99 }),
                make({ displayName: 'First', priority: 1 }),
                make({ displayName: 'Middle', priority: 50 }),
            ];

            const result = sortByPriorityAndDisplayName(input);

            expect(result).toEqual([
                make({ displayName: 'First', priority: 1 }),
                make({ displayName: 'Middle', priority: 50 }),
                make({ displayName: 'Last', priority: 99 }),
            ]);
        });

        it('handles all items without priority', () => {
            const input: SourceConfig[] = [
                make({ displayName: 'Charlie' }),
                make({ displayName: 'Alpha' }),
                make({ displayName: 'Beta' }),
            ];

            const result = sortByPriorityAndDisplayName(input);

            expect(result).toEqual([
                make({ displayName: 'Alpha' }),
                make({ displayName: 'Beta' }),
                make({ displayName: 'Charlie' }),
            ]);
        });

        it('preserves order for identical priority and displayName', () => {
            const item1 = make({ name: '1', displayName: 'Same', priority: 1 });
            const item2 = make({ name: '2', displayName: 'Same', priority: 1 });
            const item3 = make({ name: '3', displayName: 'Same' });
            const item4 = make({ name: '4', displayName: 'Same' });

            const input = [item2, item1, item4, item3];
            const result = sortByPriorityAndDisplayName(input);

            expect(result[0]).toBe(item2);
            expect(result[1]).toBe(item1);
            expect(result[2]).toBe(item4);
            expect(result[3]).toBe(item3);
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

        it('should return null for normal sources', () => {
            expect(getPillLabel(make({ name: 'A' }))).toBeNull();
        });
    });
});
