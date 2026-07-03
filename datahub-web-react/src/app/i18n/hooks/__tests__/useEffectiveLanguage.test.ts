import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

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
    });

    it('returns DEFAULT_LANGUAGE when i18n is disabled', () => {
        mockUseIsI18nEnabled.mockReturnValue(false);
        mockUseUserLanguage.mockReturnValue('en');

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe(DEFAULT_LANGUAGE);
    });

    it('returns user language when i18n is enabled and language is supported', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('en');

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe('en');
    });

    it('returns DEFAULT_LANGUAGE when i18n is enabled but language is unsupported', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('unsupported');

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe(DEFAULT_LANGUAGE);
    });

    it('returns DEFAULT_LANGUAGE when i18n is enabled but language is null', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue(null);

        const { result } = renderHook(() => useEffectiveLanguage());

        expect(result.current).toBe(DEFAULT_LANGUAGE);
    });

    it('updates when language changes', () => {
        mockUseIsI18nEnabled.mockReturnValue(true);
        mockUseUserLanguage.mockReturnValue('en');

        const { result, rerender } = renderHook(() => useEffectiveLanguage());
        expect(result.current).toBe('en');

        mockUseUserLanguage.mockReturnValue('de');
        rerender();
        expect(result.current).toBe('de');
    });
});
