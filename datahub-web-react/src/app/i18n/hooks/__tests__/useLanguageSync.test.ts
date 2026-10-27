import { renderHook } from '@testing-library/react-hooks';
import i18next from 'i18next';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { LOCALE_MAP } from '@app/i18n/constants';
import { useLanguageSync } from '@app/i18n/hooks/useLanguageSync';
import { useLocaleConfig } from '@app/i18n/hooks/useLocaleConfig';
import { hasLocaleBundleFailed } from '@src/i18n/localeBackend';
import { setDayjsLocale } from '@utils/dayjs';

vi.mock('@app/i18n/hooks/useLocaleConfig');
vi.mock('i18next', () => ({ default: { language: 'en', changeLanguage: vi.fn() } }));
vi.mock('@utils/dayjs', () => ({ setDayjsLocale: vi.fn().mockResolvedValue(undefined) }));
vi.mock('@src/i18n/localeBackend', () => ({ hasLocaleBundleFailed: vi.fn().mockReturnValue(false) }));

const mockUseLocaleConfig = vi.mocked(useLocaleConfig);
const mockHasLocaleBundleFailed = vi.mocked(hasLocaleBundleFailed);

describe('useLanguageSync', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockHasLocaleBundleFailed.mockReturnValue(false);
    });

    it('does not change i18next when its language is already current', () => {
        mockUseLocaleConfig.mockReturnValue(LOCALE_MAP.en);

        renderHook(() => useLanguageSync());

        expect(i18next.changeLanguage).not.toHaveBeenCalled();
        expect(setDayjsLocale).toHaveBeenCalledWith('en');
    });

    it('re-syncs when locale config changes', () => {
        mockUseLocaleConfig.mockReturnValue(LOCALE_MAP.en);

        const { rerender } = renderHook(() => useLanguageSync());
        expect(i18next.changeLanguage).not.toHaveBeenCalled();

        mockUseLocaleConfig.mockReturnValue(LOCALE_MAP.de);
        rerender();

        expect(i18next.changeLanguage).toHaveBeenCalledWith('de');
        expect(setDayjsLocale).toHaveBeenCalledWith('de');
    });

    it('re-requests the current language when its bundle failed to load', () => {
        mockUseLocaleConfig.mockReturnValue(LOCALE_MAP.en);
        mockHasLocaleBundleFailed.mockReturnValue(true);

        renderHook(() => useLanguageSync());

        expect(i18next.changeLanguage).toHaveBeenCalledWith('en');
    });
});
