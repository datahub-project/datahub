import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import { useAppConfig } from '@app/useAppConfig';

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

describe('useIngestionOnboardingRedesignV1', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return the feature flag value from app config when loaded', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: {
                featureFlags: {
                    ingestionOnboardingRedesignV1: true,
                },
            },
        });

        const { result } = renderHook(() => useIngestionOnboardingRedesignV1());
        expect(result.current).toBe(true);
    });

    it('should return false when feature flag is disabled in app config', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: {
                featureFlags: {
                    ingestionOnboardingRedesignV1: false,
                },
            },
        });

        const { result } = renderHook(() => useIngestionOnboardingRedesignV1());
        expect(result.current).toBe(false);
    });

    it('should return undefined if feature flag is not present in config', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: {
                featureFlags: {},
            },
        });

        const { result } = renderHook(() => useIngestionOnboardingRedesignV1());
        expect(result.current).toBe(undefined);
    });
});
