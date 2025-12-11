/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { getUniqueItemsByKey } from '@app/searchV2/filtersV2/utils';

describe('getUniqueItemsByKey', () => {
    it('returns unique items based on a keyAccessor function', () => {
        const items = [
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 1, name: 'Charlie' },
        ];
        const keyAccessor = (item: { id: number; name: string }) => String(item.id);

        const result = getUniqueItemsByKey(items, keyAccessor);

        expect(result).toHaveLength(2);
        expect(result).toEqual([
            { id: 1, name: 'Charlie' },
            { id: 2, name: 'Bob' },
        ]);
    });

    it('handles empty array input', () => {
        const items: Array<{ id: number; name: string }> = [];
        const keyAccessor = (item: { id: number; name: string }) => String(item.id);

        const result = getUniqueItemsByKey(items, keyAccessor);

        expect(result).toHaveLength(0);
        expect(result).toEqual([]);
    });

    it('handles all unique items', () => {
        const items = [
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 3, name: 'Charlie' },
        ];
        const keyAccessor = (item: { id: number; name: string }) => String(item.id);

        const result = getUniqueItemsByKey(items, keyAccessor);

        expect(result).toHaveLength(3);
        expect(result).toEqual(items);
    });

    it('handles non-primitive keys (e.g., objects)', () => {
        const items = [
            { id: { subId: 1 }, name: 'Alice' },
            { id: { subId: 2 }, name: 'Bob' },
            { id: { subId: 1 }, name: 'Charlie' },
        ];
        const keyAccessor = (item: { id: { subId: number }; name: string }) => String(item.id.subId);

        const result = getUniqueItemsByKey(items, keyAccessor);

        expect(result).toHaveLength(2);
        expect(result).toEqual([
            { id: { subId: 1 }, name: 'Charlie' },
            { id: { subId: 2 }, name: 'Bob' },
        ]);
    });

    it('handles mixed primitive and object keys', () => {
        const items = [
            { id: 1, name: 'Alice' },
            { id: 'two', name: 'Bob' },
            { id: 1, name: 'Charlie' },
        ];
        const keyAccessor = (item: { id: number | string; name: string }) => String(item.id);

        const result = getUniqueItemsByKey(items, keyAccessor);

        expect(result).toHaveLength(2);
        expect(result).toEqual([
            { id: 1, name: 'Charlie' },
            { id: 'two', name: 'Bob' },
        ]);
    });

    it('handles null or undefined values in the array', () => {
        const items = [{ id: 1, name: 'Alice' }, null, undefined, { id: 1, name: 'Charlie' }];
        const keyAccessor = (item: { id: number; name: string } | null | undefined) =>
            item?.id !== undefined ? String(item.id) : '';

        const result = getUniqueItemsByKey(items, keyAccessor);

        expect(result).toHaveLength(2);
        expect(result).toEqual([{ id: 1, name: 'Charlie' }, undefined]);
    });
});
