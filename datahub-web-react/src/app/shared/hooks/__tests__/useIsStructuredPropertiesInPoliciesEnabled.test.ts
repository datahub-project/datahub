import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useIsStructuredPropertiesInPoliciesEnabled } from '@app/shared/hooks/useIsStructuredPropertiesInPoliciesEnabled';
import { useAppConfig } from '@app/useAppConfig';

const localStorageMock = (() => {
    let store: Record<string, string> = {};
    return {
        getItem: (key: string) => store[key] ?? null,
        setItem: (key: string, value: string) => {
            store[key] = value;
        },
        clear: () => {
            store = {};
        },
    };
})();
Object.defineProperty(window, 'localStorage', { value: localStorageMock });

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

describe('useIsStructuredPropertiesInPoliciesEnabled', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        localStorage.clear();
    });

    it('should return true when structuredPropertiesInPoliciesEnabled flag is true and config is loaded', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: true,
            config: { featureFlags: { structuredPropertiesInPoliciesEnabled: true } },
        });

        const { result } = renderHook(() => useIsStructuredPropertiesInPoliciesEnabled());

        expect(result.current).toBe(true);
    });

    it('should return false when structuredPropertiesInPoliciesEnabled flag is false and config is loaded', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: true,
            config: { featureFlags: { structuredPropertiesInPoliciesEnabled: false } },
        });

        const { result } = renderHook(() => useIsStructuredPropertiesInPoliciesEnabled());

        expect(result.current).toBe(false);
    });

    it('should return cached localStorage value when config is not yet loaded', () => {
        localStorage.setItem('structuredPropertiesInPoliciesEnabled', 'true');

        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: false,
            config: { featureFlags: { structuredPropertiesInPoliciesEnabled: false } },
        });

        const { result } = renderHook(() => useIsStructuredPropertiesInPoliciesEnabled());

        expect(result.current).toBe(true);
    });

    it('should return false when config is not loaded and localStorage has no value', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: false,
            config: { featureFlags: {} },
        });

        const { result } = renderHook(() => useIsStructuredPropertiesInPoliciesEnabled());

        expect(result.current).toBe(false);
    });

    it('should persist the flag value to localStorage when config is loaded', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: true,
            config: { featureFlags: { structuredPropertiesInPoliciesEnabled: true } },
        });

        renderHook(() => useIsStructuredPropertiesInPoliciesEnabled());

        expect(localStorage.getItem('structuredPropertiesInPoliciesEnabled')).toBe('true');
    });
});
