import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DEFAULT_LANGUAGE } from '@app/i18n/constants';
import { useEffectiveLanguage } from '@app/i18n/hooks/useEffectiveLanguage';
import { useIsI18nEnabled } from '@app/i18n/hooks/useIsI18nEnabled';
import { detectBrowserLanguage } from '@app/i18n/utils';
import { useUserLanguage } from '@app/shared/hooks/useUserLanguage';

vi.mock('@app/i18n/hooks/useIsI18nEnabled');
vi.mock('@app/shared/hooks/useUserLanguage');
vi.mock('@app/i18n/utils', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@app/i18n/utils')>();
    return { ...actual, detectBrowserLanguage: vi.fn() };
});

const mockUseIsI18nEnabled = vi.mocked(useIsI18nEnabled);
const mockUseUserLanguage = vi.mocked(useUserLanguage);
const mockDetectBrowserLanguage = vi.mocked(detectBrowserLanguage);

describe('useEffectiveLanguage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockDetectBrowserLanguage.mockReturnValue(undefined);
    });

    it('returns DEFAULT_LANGUAGE when i18n is disabled, even if the browser language is supported', () => {
        mockUseIsI18nEnabled.mockReturnValue(false);
        mockUseUserLanguage.mockReturnValue('de');
        mockDetectBrowserLanguage.mockReturnValue('de');

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe(DEFAULT_LANGUAGE);
    });

    it('returns the user language when i18n is enabled and it is supported', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('de');

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe('de');
    });

    it('prefers the in-app user language over the browser language', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('de');
        mockDetectBrowserLanguage.mockReturnValue('fr');

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe('de');
    });

    it('falls back to the browser language when the user has no supported preference', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue(null);
        mockDetectBrowserLanguage.mockReturnValue('fr');

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe('fr');
    });

    it('falls back to DEFAULT_LANGUAGE when neither the user nor the browser language is supported', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('unsupported');
        mockDetectBrowserLanguage.mockReturnValue(undefined);

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe(DEFAULT_LANGUAGE);
    });

    it('updates when the user language changes', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('en');

        const { result, rerender } = renderHook(() => useEffectiveLanguage());
        expect(result.current).toBe('en');

        mockUseUserLanguage.mockReturnValue('de');
        rerender();
        expect(result.current).toBe('de');
    });
});
