import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import { useGetIngestionLink } from '@app/sharedV2/ingestionSources/useGetIngestionLink';
import { PageRoutes } from '@conf/Global';

vi.mock('@app/ingestV2/hooks/useIngestionOnboardingRedesignV1', () => ({
    useIngestionOnboardingRedesignV1: vi.fn(),
}));

describe('useGetIngestionLink', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return INGESTION when feature flag is off and hasSources = true', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(false);

        const { result } = renderHook(() => useGetIngestionLink(true));

        expect(result.current).toBe(PageRoutes.INGESTION);
    });

    it('should return INGESTION when feature flag is off and hasSources = false', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(false);

        const { result } = renderHook(() => useGetIngestionLink(false));

        expect(result.current).toBe(PageRoutes.INGESTION);
    });

    it('should return INGESTION when feature flag is on and ingestion sources exist', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(true);

        const { result } = renderHook(() => useGetIngestionLink(true));

        expect(result.current).toBe(PageRoutes.INGESTION);
    });

    it('should return INGESTION_CREATE when feature flag is on and no ingestion sources exist', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(true);

        const { result } = renderHook(() => useGetIngestionLink(false));

        expect(result.current).toBe(PageRoutes.INGESTION_CREATE);
    });

    it('should not throw when called multiple times', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(true);

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
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(true);

        const { result } = renderHook(() => useGetIngestionLink(false));

        expect(typeof result.current).toBe('string');
        expect(result.current.length).toBeGreaterThan(0);
    });

    it('should remain stable across re-renders when flag does not change', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(true);

        const { result, rerender } = renderHook(({ val }) => useGetIngestionLink(val), { initialProps: { val: true } });

        const firstValue = result.current;

        rerender({ val: true });
        expect(result.current).toBe(firstValue);
    });

    it('should not crash when invoked with any random boolean', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(true);

        expect(() => renderHook(() => useGetIngestionLink(Math.random() > 0.5))).not.toThrow();
    });

    it('should prioritize feature flag logic over default branch', () => {
        (useIngestionOnboardingRedesignV1 as any).mockReturnValue(true);

        const { result } = renderHook(() => useGetIngestionLink(false));

        expect(result.current).toBe(PageRoutes.INGESTION_CREATE);
    });
});
