import { describe, expect, it } from 'vitest';

import { useOperandKeys } from '@app/sharedV2/queryBuilder/Operands.hooks';
import { renderHook } from '@utils/test-utils/renderHook';

describe('useOperandKeys', () => {
    it('keeps existing keys and appends new ones when operands are added', () => {
        const { result, rerender } = renderHook(({ count }) => useOperandKeys(count), { initialProps: { count: 2 } });
        const [first, second] = result.current.keys;

        rerender({ count: 3 });

        expect(result.current.keys.slice(0, 2)).toEqual([first, second]);
        expect(new Set(result.current.keys).size).toBe(3);
    });

    it('drops the key of the operand marked for removal', () => {
        const { result, rerender } = renderHook(({ count }) => useOperandKeys(count), { initialProps: { count: 3 } });
        const [first, , third] = result.current.keys;

        result.current.markRemoval(1);
        rerender({ count: 2 });

        expect(result.current.keys).toEqual([first, third]);
    });

    it('keeps the leading keys when operands shrink without a marked removal', () => {
        const { result, rerender } = renderHook(({ count }) => useOperandKeys(count), { initialProps: { count: 3 } });
        const [first, second] = result.current.keys;

        rerender({ count: 2 });

        expect(result.current.keys).toEqual([first, second]);
    });

    it('ignores a marked removal once the operands have changed in another way', () => {
        const { result, rerender } = renderHook(({ count }) => useOperandKeys(count), { initialProps: { count: 3 } });
        const [first, second, third] = result.current.keys;

        // The removal was rejected by the parent, then an operand was added and one removed elsewhere.
        result.current.markRemoval(0);
        rerender({ count: 4 });
        rerender({ count: 3 });

        expect(result.current.keys).toEqual([first, second, third]);
    });
});
