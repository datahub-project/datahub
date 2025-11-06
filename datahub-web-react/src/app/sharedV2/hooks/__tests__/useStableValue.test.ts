import { renderHook } from '@testing-library/react-hooks';

import { useStableValue } from '@app/sharedV2/hooks/useStableValue';

describe('useStableValue', () => {
    it('should return the initial value', () => {
        const { result } = renderHook(() => useStableValue(1));
        expect(result.current).toBe(1);
    });

    it('should return the same value if the new value is deeply equal', () => {
        const initialValue = { a: 1, b: { c: 2 } };
        const { result, rerender } = renderHook(({ value }) => useStableValue(value), {
            initialProps: { value: initialValue },
        });

        expect(result.current).toBe(initialValue);

        const newValue = { a: 1, b: { c: 2 } }; // Deeply equal, but new reference
        rerender({ value: newValue });

        expect(result.current).toBe(initialValue); // Should still be the initial reference
    });

    it('should return a new value if the new value is not deeply equal', () => {
        const initialValue = { a: 1, b: { c: 2 } };
        const { result, rerender } = renderHook(({ value }) => useStableValue(value), {
            initialProps: { value: initialValue },
        });

        expect(result.current).toBe(initialValue);

        const newValue = { a: 1, b: { c: 3 } }; // Not deeply equal
        rerender({ value: newValue });

        expect(result.current).toBe(newValue); // Should be the new reference
    });

    it('should handle primitive values correctly', () => {
        const { result, rerender } = renderHook(({ value }) => useStableValue(value), {
            initialProps: { value: 1 },
        });

        expect(result.current).toBe(1);

        rerender({ value: 1 });
        expect(result.current).toBe(1);

        rerender({ value: 2 });
        expect(result.current).toBe(2);
    });

    it('should use a custom comparison function if provided', () => {
        const customCompare = (a: { id: string }, b: { id: string }) => a.id === b.id;
        const initialValue = { id: '1', name: 'test1' };
        const { result, rerender } = renderHook(({ value }) => useStableValue(value, customCompare), {
            initialProps: { value: initialValue },
        });

        expect(result.current).toBe(initialValue);

        const newValueSameId = { id: '1', name: 'test2' }; // Different name, but same ID
        rerender({ value: newValueSameId });
        expect(result.current).toBe(initialValue); // Should still be initial reference due to customCompare

        const newValueDifferentId = { id: '2', name: 'test3' }; // Different ID
        rerender({ value: newValueDifferentId });
        expect(result.current).toBe(newValueDifferentId); // Should be new reference
    });

    it('should handle undefined and null values', () => {
        const { result, rerender } = renderHook(({ value }) => useStableValue(value), {
            initialProps: { value: undefined },
        });

        expect(result.current).toBe(undefined);

        rerender({ value: null } as any);
        expect(result.current).toBe(null);

        rerender({ value: undefined });
        expect(result.current).toBe(undefined);
    });
});
