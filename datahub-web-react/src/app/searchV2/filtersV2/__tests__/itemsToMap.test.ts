import { itemsToMap } from '../utils';

describe('itemsToMap', () => {
    it('creates a map without a keyAccessor (uses item as key)', () => {
        const items = ['a', 'b', 'c'];
        const result = itemsToMap(items);

        expect(result.size).toBe(3);
        expect(result.get('a')).toBe('a');
        expect(result.get('b')).toBe('b');
        expect(result.get('c')).toBe('c');
    });

    it('creates a map with a keyAccessor function', () => {
        const items = [
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 3, name: 'Charlie' },
        ];
        const keyAccessor = (item: { id: number; name: string }) => item.id;
        const result = itemsToMap(items, keyAccessor);

        expect(result.size).toBe(3);
        expect(result.get(1)).toEqual({ id: 1, name: 'Alice' });
        expect(result.get(2)).toEqual({ id: 2, name: 'Bob' });
        expect(result.get(3)).toEqual({ id: 3, name: 'Charlie' });
    });

    it('handles duplicate keys by overwriting the value', () => {
        const items = [
            { id: 1, name: 'Alice' },
            { id: 1, name: 'Bob' },
            { id: 2, name: 'Charlie' },
        ];
        const keyAccessor = (item: { id: number; name: string }) => item.id;
        const result = itemsToMap(items, keyAccessor);

        expect(result.size).toBe(2);
        expect(result.get(1)).toEqual({ id: 1, name: 'Bob' });
        expect(result.get(2)).toEqual({ id: 2, name: 'Charlie' });
    });

    it('handles empty array input', () => {
        const items: Array<number> = [];
        const result = itemsToMap(items);

        expect(result.size).toBe(0);
    });

    it('handles objects with non-primitive keys', () => {
        const items = [
            { id: { subId: 1 }, name: 'Alice' },
            { id: { subId: 2 }, name: 'Bob' },
        ];
        const keyAccessor = (item: { id: { subId: number }; name: string }) => item.id;
        const result = itemsToMap(items, keyAccessor);

        expect(result.size).toBe(2);
        expect(result.get(items[0].id)).toEqual({ id: { subId: 1 }, name: 'Alice' });
        expect(result.get(items[1].id)).toEqual({ id: { subId: 2 }, name: 'Bob' });
    });

    it('handles mixed primitive and object keys', () => {
        const items = [
            { id: 1, name: 'Alice' },
            { id: 'two', name: 'Bob' },
        ];
        const keyAccessor = (item: { id: number | string; name: string }) => item.id;
        const result = itemsToMap(items, keyAccessor);

        expect(result.size).toBe(2);
        expect(result.get(1)).toEqual({ id: 1, name: 'Alice' });
        expect(result.get('two')).toEqual({ id: 'two', name: 'Bob' });
    });

    it('handles null or undefined values gracefully', () => {
        const items = [null, undefined];
        const result = itemsToMap(items);

        expect(result.size).toBe(2);
        expect(result.get(null)).toBeNull();
        expect(result.get(undefined)).toBeUndefined();
    });
});
