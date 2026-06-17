import { renderHook } from '@testing-library/react-hooks';
import i18next from 'i18next';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { LOCALE_MAP } from '@app/i18n/constants';
import { useLanguageSync } from '@app/i18n/hooks/useLanguageSync';
import { useLocaleConfig } from '@app/i18n/hooks/useLocaleConfig';
import dayjs from '@utils/dayjs';

vi.mock('@app/i18n/hooks/useLocaleConfig');
vi.mock('i18next', () => ({ default: { changeLanguage: vi.fn() } }));
vi.mock('@utils/dayjs', () => ({ default: { locale: vi.fn() } }));

const mockUseLocaleConfig = vi.mocked(useLocaleConfig);

describe('useLanguageSync', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('syncs i18next and dayjs when locale config changes', () => {
        mockUseLocaleConfig.mockReturnValue(LOCALE_MAP.en);

        renderHook(() => useLanguageSync());

        expect(i18next.changeLanguage).toHaveBeenCalledWith('en');
        expect(dayjs.locale).toHaveBeenCalledWith('en');
    });

    it('re-syncs when locale config changes', () => {
        mockUseLocaleConfig.mockReturnValue(LOCALE_MAP.en);

        const { rerender } = renderHook(() => useLanguageSync());
        expect(i18next.changeLanguage).toHaveBeenCalledWith('en');

        mockUseLocaleConfig.mockReturnValue(LOCALE_MAP.de);
        rerender();

        expect(i18next.changeLanguage).toHaveBeenCalledWith('de');
        expect(dayjs.locale).toHaveBeenCalledWith('de');
    });
});
