import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useShowIngestionOnboardingRedesign } from '@app/ingestV2/hooks/useShowIngestionOnboardingRedesign';
import { useAppConfig } from '@app/useAppConfig';

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

describe('useShowIngestionOnboardingRedesign', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return the feature flag value from app config when loaded', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: {
                featureFlags: {
                    showIngestionOnboardingRedesign: true,
                },
            },
        });

        const { result } = renderHook(() => useShowIngestionOnboardingRedesign());
        expect(result.current).toBe(true);
    });

    it('should return false when feature flag is disabled in app config', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: {
                featureFlags: {
                    showIngestionOnboardingRedesign: false,
                },
            },
        });

        const { result } = renderHook(() => useShowIngestionOnboardingRedesign());
        expect(result.current).toBe(false);
    });

    it('should return undefined if feature flag is not present in config', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: {
                featureFlags: {},
            },
        });

        const { result } = renderHook(() => useShowIngestionOnboardingRedesign());
        expect(result.current).toBe(undefined);
    });
});
