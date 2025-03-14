import { getUniqueItemsByKeyFromArrrays } from '../utils';

describe('getUniqueItemsByKeyFromArrrays', () => {
    it('deduplicates items across multiple arrays based on a keyAccessor', () => {
        const arrays = [
            [
                { id: 1, name: 'Alice' },
                { id: 2, name: 'Bob' },
            ],
            [
                { id: 1, name: 'Charlie' },
                { id: 3, name: 'David' },
            ],
        ];
        const keyAccessor = (item: { id: number; name: string }) => item.id;

        const result = getUniqueItemsByKeyFromArrrays(arrays, keyAccessor);

        expect(result).toHaveLength(3);
        expect(result).toEqual([
            { id: 1, name: 'Charlie' },
            { id: 2, name: 'Bob' },
            { id: 3, name: 'David' },
        ]);
    });

    it('handles empty arrays input', () => {
        const arrays: Array<Array<{ id: number; name: string }>> = [[], []];
        const keyAccessor = (item: { id: number; name: string }) => item.id;

        const result = getUniqueItemsByKeyFromArrrays(arrays, keyAccessor);

        expect(result).toHaveLength(0);
        expect(result).toEqual([]);
    });

    it('handles arrays with all unique items', () => {
        const arrays = [
            [
                { id: 1, name: 'Alice' },
                { id: 2, name: 'Bob' },
            ],
            [
                { id: 3, name: 'Charlie' },
                { id: 4, name: 'David' },
            ],
        ];
        const keyAccessor = (item: { id: number; name: string }) => item.id;

        const result = getUniqueItemsByKeyFromArrrays(arrays, keyAccessor);

        expect(result).toHaveLength(4);
        expect(result).toEqual([
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 3, name: 'Charlie' },
            { id: 4, name: 'David' },
        ]);
    });

    it('handles non-primitive keys (e.g., objects)', () => {
        const arrays = [
            [
                { id: { subId: 1 }, name: 'Alice' },
                { id: { subId: 2 }, name: 'Bob' },
            ],
            [
                { id: { subId: 1 }, name: 'Charlie' },
                { id: { subId: 3 }, name: 'David' },
            ],
        ];
        const keyAccessor = (item: { id: { subId: number }; name: string }) => item.id.subId;

        const result = getUniqueItemsByKeyFromArrrays(arrays, keyAccessor);

        expect(result).toHaveLength(3);
        expect(result).toEqual([
            { id: { subId: 1 }, name: 'Charlie' },
            { id: { subId: 2 }, name: 'Bob' },
            { id: { subId: 3 }, name: 'David' },
        ]);
    });

    it('handles mixed primitive and object keys', () => {
        const arrays = [
            [
                { id: 1, name: 'Alice' },
                { id: 'two', name: 'Bob' },
            ],
            [
                { id: 1, name: 'Charlie' },
                { id: 'three', name: 'David' },
            ],
        ];
        const keyAccessor = (item: { id: number | string; name: string }) => item.id;

        const result = getUniqueItemsByKeyFromArrrays(arrays, keyAccessor);

        expect(result).toHaveLength(3);
        expect(result).toEqual([
            { id: 1, name: 'Charlie' },
            { id: 'two', name: 'Bob' },
            { id: 'three', name: 'David' },
        ]);
    });

    it('handles null or undefined values in arrays', () => {
        const arrays = [
            [{ id: 1, name: 'Alice' }, null, undefined],
            [{ id: 1, name: 'Charlie' }, null, undefined],
        ];
        const keyAccessor = (item: { id: number; name: string } | null | undefined) =>
            item?.id !== undefined ? item.id : '';

        const result = getUniqueItemsByKeyFromArrrays(arrays, keyAccessor);

        expect(result).toHaveLength(2);
        expect(result).toEqual([{ id: 1, name: 'Charlie' }, undefined]);
    });

    it('handles no keyAccessor (uses item as key)', () => {
        const arrays = [
            ['a', 'b'],
            ['a', 'c'],
        ];

        const result = getUniqueItemsByKeyFromArrrays(arrays);

        expect(result).toHaveLength(3);
        expect(result).toEqual(['a', 'b', 'c']);
    });
});
