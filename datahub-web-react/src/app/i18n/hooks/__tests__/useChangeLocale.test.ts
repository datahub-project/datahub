import { act, renderHook } from '@testing-library/react-hooks';
import i18next from 'i18next';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DEFAULT_LANGUAGE } from '@app/i18n/constants';
import { useChangeLocale } from '@app/i18n/hooks/useChangeLocale';
import { useUpdateUserLocaleSettings } from '@app/i18n/hooks/useUpdateUserLocaleSettings';
import { setDayjsLocale } from '@utils/dayjs';

vi.mock('@app/i18n/hooks/useUpdateUserLocaleSettings');
vi.mock('@utils/dayjs', () => ({ setDayjsLocale: vi.fn().mockResolvedValue(undefined) }));

const mockUpdateUserLocaleSettings = vi.fn().mockResolvedValue({});
vi.mocked(useUpdateUserLocaleSettings).mockReturnValue(mockUpdateUserLocaleSettings);

describe('useChangeLocale', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(i18next, 'changeLanguage').mockResolvedValue('en' as any);
        vi.mocked(useUpdateUserLocaleSettings).mockReturnValue(mockUpdateUserLocaleSettings);
    });

    it('updates locale to the given supported language', async () => {
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await result.current('en');
        });

        expect(mockUpdateUserLocaleSettings).toHaveBeenCalledWith('en');
        expect(i18next.changeLanguage).toHaveBeenCalledWith('en');
        expect(setDayjsLocale).toHaveBeenCalledWith('en');
    });

    it('falls back to DEFAULT_LANGUAGE for unsupported language', async () => {
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await result.current('unsupported');
        });

        expect(mockUpdateUserLocaleSettings).toHaveBeenCalledWith('unsupported');
        expect(i18next.changeLanguage).toHaveBeenCalledWith(DEFAULT_LANGUAGE);
        expect(setDayjsLocale).toHaveBeenCalledWith(DEFAULT_LANGUAGE);
    });

    it('falls back to DEFAULT_LANGUAGE for null', async () => {
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await result.current(null);
        });

        expect(mockUpdateUserLocaleSettings).toHaveBeenCalledWith(null);
        expect(i18next.changeLanguage).toHaveBeenCalledWith(DEFAULT_LANGUAGE);
    });
});
