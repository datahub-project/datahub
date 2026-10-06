import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { DEFAULT_LANGUAGE } from '@app/i18n/constants';
import { useEffectiveLanguage } from '@app/i18n/hooks/useEffectiveLanguage';
import { useIsI18nEnabled } from '@app/i18n/hooks/useIsI18nEnabled';
import { useUserLanguage } from '@app/shared/hooks/useUserLanguage';

vi.mock('@app/i18n/hooks/useIsI18nEnabled');
vi.mock('@app/shared/hooks/useUserLanguage');

const mockUseIsI18nEnabled = vi.mocked(useIsI18nEnabled);
const mockUseUserLanguage = vi.mocked(useUserLanguage);

describe('useEffectiveLanguage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.stubGlobal('navigator', { languages: [], language: undefined });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('returns DEFAULT_LANGUAGE when i18n is disabled, even if the browser language is supported', () => {
        mockUseIsI18nEnabled.mockReturnValue(false);
        mockUseUserLanguage.mockReturnValue('de');
        vi.stubGlobal('navigator', { languages: ['de'], language: 'de' });

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
        vi.stubGlobal('navigator', { languages: ['fr'], language: 'fr' });

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe('de');
    });

    it('falls back to the browser language when the user has no supported preference', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue(null);
        vi.stubGlobal('navigator', { languages: ['fr'], language: 'fr' });

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe('fr');
    });

    it('falls back to DEFAULT_LANGUAGE when neither the user nor the browser language is supported', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('unsupported');
        vi.stubGlobal('navigator', { languages: ['ko-KR'], language: 'ko-KR' });

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
