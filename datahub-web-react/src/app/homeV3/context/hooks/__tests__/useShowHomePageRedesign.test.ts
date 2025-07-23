import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useAppConfig } from '@app/useAppConfig';

import { loadHomePageRedesignFromLocalStorage, useShowHomePageRedesign } from '../useShowHomePageRedesign';

// Mock useAppConfig
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

const mockUseAppConfig = vi.mocked(useAppConfig);

// Mock localStorage
const localStorageMock = {
    getItem: vi.fn(),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
};

Object.defineProperty(window, 'localStorage', {
    value: localStorageMock,
});

describe('useShowHomePageRedesign', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        localStorageMock.getItem.mockClear();
        localStorageMock.setItem.mockClear();
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    describe('when app config is loaded', () => {
        it('should return true when feature flag is enabled', () => {
            mockUseAppConfig.mockReturnValue({
                loaded: true,
                config: {
                    featureFlags: {
                        showHomePageRedesign: true,
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue('false'); // Different from feature flag

            const { result } = renderHook(() => useShowHomePageRedesign());

            expect(result.current).toBe(true);
            expect(localStorageMock.setItem).toHaveBeenCalledWith('showHomePageRedesign', 'true');
        });

        it('should return false when feature flag is disabled', () => {
            mockUseAppConfig.mockReturnValue({
                loaded: true,
                config: {
                    featureFlags: {
                        showHomePageRedesign: false,
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue('true'); // Different from feature flag

            const { result } = renderHook(() => useShowHomePageRedesign());

            expect(result.current).toBe(false);
            expect(localStorageMock.setItem).toHaveBeenCalledWith('showHomePageRedesign', 'false');
        });

        it('should not update localStorage when value is already the same', () => {
            mockUseAppConfig.mockReturnValue({
                loaded: true,
                config: {
                    featureFlags: {
                        showHomePageRedesign: true,
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue('true'); // Same as feature flag

            const { result } = renderHook(() => useShowHomePageRedesign());

            expect(result.current).toBe(true);
            expect(localStorageMock.setItem).not.toHaveBeenCalled();
        });
    });

    describe('when app config is not loaded', () => {
        it('should return localStorage value when localStorage has true', () => {
            mockUseAppConfig.mockReturnValue({
                loaded: false,
                config: {
                    featureFlags: {
                        showHomePageRedesign: false, // This should be ignored
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue('true');

            const { result } = renderHook(() => useShowHomePageRedesign());

            expect(result.current).toBe(true);
            expect(localStorageMock.setItem).not.toHaveBeenCalled();
        });

        it('should return localStorage value when localStorage has false', () => {
            mockUseAppConfig.mockReturnValue({
                loaded: false,
                config: {
                    featureFlags: {
                        showHomePageRedesign: true, // This should be ignored
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue('false');

            const { result } = renderHook(() => useShowHomePageRedesign());

            expect(result.current).toBe(false);
            expect(localStorageMock.setItem).not.toHaveBeenCalled();
        });

        it('should return false when localStorage is null', () => {
            mockUseAppConfig.mockReturnValue({
                loaded: false,
                config: {
                    featureFlags: {
                        showHomePageRedesign: true,
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue(null);

            const { result } = renderHook(() => useShowHomePageRedesign());

            expect(result.current).toBe(false);
            expect(localStorageMock.setItem).not.toHaveBeenCalled();
        });

        it('should return false when localStorage has invalid value', () => {
            mockUseAppConfig.mockReturnValue({
                loaded: false,
                config: {
                    featureFlags: {
                        showHomePageRedesign: true,
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue('invalid');

            const { result } = renderHook(() => useShowHomePageRedesign());

            expect(result.current).toBe(false);
            expect(localStorageMock.setItem).not.toHaveBeenCalled();
        });
    });

    describe('state transitions', () => {
        it('should update from localStorage to feature flag when config becomes loaded', () => {
            // Start with config not loaded
            mockUseAppConfig.mockReturnValue({
                loaded: false,
                config: {
                    featureFlags: {
                        showHomePageRedesign: false,
                    },
                },
            } as any);

            localStorageMock.getItem.mockReturnValue('true');

            const { result, rerender } = renderHook(() => useShowHomePageRedesign());

            // Initially should return localStorage value
            expect(result.current).toBe(true);

            // Update to config loaded
            mockUseAppConfig.mockReturnValue({
                loaded: true,
                config: {
                    featureFlags: {
                        showHomePageRedesign: false,
                    },
                },
            } as any);

            rerender();

            // Should now return feature flag value
            expect(result.current).toBe(false);
            expect(localStorageMock.setItem).toHaveBeenCalledWith('showHomePageRedesign', 'false');
        });
    });
});

describe('loadHomePageRedesignFromLocalStorage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        localStorageMock.getItem.mockClear();
    });

    it('should return true when localStorage value is "true"', () => {
        localStorageMock.getItem.mockReturnValue('true');

        const result = loadHomePageRedesignFromLocalStorage();

        expect(result).toBe(true);
        expect(localStorageMock.getItem).toHaveBeenCalledWith('showHomePageRedesign');
    });

    it('should return false when localStorage value is "false"', () => {
        localStorageMock.getItem.mockReturnValue('false');

        const result = loadHomePageRedesignFromLocalStorage();

        expect(result).toBe(false);
        expect(localStorageMock.getItem).toHaveBeenCalledWith('showHomePageRedesign');
    });

    it('should return false when localStorage value is null', () => {
        localStorageMock.getItem.mockReturnValue(null);

        const result = loadHomePageRedesignFromLocalStorage();

        expect(result).toBe(false);
        expect(localStorageMock.getItem).toHaveBeenCalledWith('showHomePageRedesign');
    });

    it('should return false when localStorage value is invalid', () => {
        localStorageMock.getItem.mockReturnValue('invalid');

        const result = loadHomePageRedesignFromLocalStorage();

        expect(result).toBe(false);
        expect(localStorageMock.getItem).toHaveBeenCalledWith('showHomePageRedesign');
    });
});
