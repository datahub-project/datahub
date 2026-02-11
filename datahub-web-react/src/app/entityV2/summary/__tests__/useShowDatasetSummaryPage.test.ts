import { renderHook } from '@testing-library/react-hooks';
import { MockedFunction, beforeEach, describe, expect, it, vi } from 'vitest';

import { useShowDatasetSummaryPage } from '@app/entityV2/summary/useShowDatasetSummaryPage';
import { useAppConfig } from '@app/useAppConfig';

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

const mockedUseAppConfig = useAppConfig as MockedFunction<typeof useAppConfig>;

describe('useShowDatasetSummaryPage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return true when loaded is true and flag is true', () => {
        mockedUseAppConfig.mockReturnValue({
            loaded: true,
            config: { featureFlags: { datasetSummaryPageV1: true } },
        } as any);

        const { result } = renderHook(() => useShowDatasetSummaryPage());
        expect(result.current).toBe(true);
    });

    it('should return false when loaded is true and flag is false', () => {
        mockedUseAppConfig.mockReturnValue({
            loaded: true,
            config: { featureFlags: { datasetSummaryPageV1: false } },
        } as any);

        const { result } = renderHook(() => useShowDatasetSummaryPage());
        expect(result.current).toBe(false);
    });

    it('should return false when loaded is false regardless of the flag', () => {
        mockedUseAppConfig.mockReturnValue({
            loaded: false,
            config: { featureFlags: { datasetSummaryPageV1: true } },
        } as any);

        const { result } = renderHook(() => useShowDatasetSummaryPage());
        expect(result.current).toBe(false);
    });

    it('should return false when loaded is true but flag is missing', () => {
        mockedUseAppConfig.mockReturnValue({
            loaded: true,
            config: { featureFlags: { datasetSummaryPageV1: false } },
        } as any);

        const { result } = renderHook(() => useShowDatasetSummaryPage());
        expect(result.current).toBe(false);
    });

    it('should return false when loaded is false and flag is missing', () => {
        mockedUseAppConfig.mockReturnValue({
            loaded: false,
            config: { featureFlags: { datasetSummaryPageV1: false } },
        } as any);

        const { result } = renderHook(() => useShowDatasetSummaryPage());
        expect(result.current).toBe(false);
    });
});
