import { describe, expect, it } from 'vitest';

import { useGetIngestionLink } from '@app/sharedV2/ingestionSources/useGetIngestionLink';
import { PageRoutes } from '@conf/Global';
import { renderHook } from '@utils/test-utils/renderHook';

describe('useGetIngestionLink', () => {
    it('should return INGESTION when ingestion sources exist', () => {
        const { result } = renderHook(() => useGetIngestionLink(true));

        expect(result.current).toBe(PageRoutes.INGESTION);
    });

    it('should return INGESTION_CREATE when no ingestion sources exist', () => {
        const { result } = renderHook(() => useGetIngestionLink(false));

        expect(result.current).toBe(PageRoutes.INGESTION_CREATE);
    });

    it('should not throw when called multiple times', () => {
        const { result, rerender } = renderHook(({ value }) => useGetIngestionLink(value), {
            initialProps: { value: true },
        });

        expect(result.current).toBe(PageRoutes.INGESTION);

        rerender({ value: false });
        expect(result.current).toBe(PageRoutes.INGESTION_CREATE);

        rerender({ value: true });
        expect(result.current).toBe(PageRoutes.INGESTION);
    });

    it('should return a valid string route always', () => {
        const { result } = renderHook(() => useGetIngestionLink(false));

        expect(typeof result.current).toBe('string');
        expect(result.current.length).toBeGreaterThan(0);
    });

    it('should remain stable across re-renders when input does not change', () => {
        const { result, rerender } = renderHook(({ val }) => useGetIngestionLink(val), { initialProps: { val: true } });

        const firstValue = result.current;

        rerender({ val: true });
        expect(result.current).toBe(firstValue);
    });

    it('should not crash when invoked with any random boolean', () => {
        expect(() => renderHook(() => useGetIngestionLink(Math.random() > 0.5))).not.toThrow();
    });
});
