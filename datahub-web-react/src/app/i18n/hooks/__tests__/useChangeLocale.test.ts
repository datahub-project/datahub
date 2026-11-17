import { act } from '@testing-library/react';
import i18next from 'i18next';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { EventType } from '@app/analytics';
import { DEFAULT_LANGUAGE } from '@app/i18n/constants';
import { useChangeLocale } from '@app/i18n/hooks/useChangeLocale';
import { useUpdateUserLocaleSettings } from '@app/i18n/hooks/useUpdateUserLocaleSettings';
import { useUserLanguage } from '@app/sharedV2/hooks/useUserLanguage';
import { setDayjsLocale } from '@utils/dayjs';
import { renderHook } from '@utils/test-utils/renderHook';

vi.mock('@app/analytics', () => ({
    __esModule: true,
    default: { event: vi.fn() },
    EventType: { LanguageChangeEvent: 'LanguageChangeEvent' },
}));
vi.mock('@app/i18n/hooks/useUpdateUserLocaleSettings');
vi.mock('@app/sharedV2/hooks/useUserLanguage');
vi.mock('@utils/dayjs', () => ({ setDayjsLocale: vi.fn().mockResolvedValue(undefined) }));

const mockUpdateUserLocaleSettings = vi.fn().mockResolvedValue({});
vi.mocked(useUpdateUserLocaleSettings).mockReturnValue(mockUpdateUserLocaleSettings);

describe('useChangeLocale', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(i18next, 'changeLanguage').mockResolvedValue('en' as any);
        vi.spyOn(i18next, 'language', 'get').mockReturnValue('en');
        vi.spyOn(navigator, 'languages', 'get').mockReturnValue(['fr-FR']);
        vi.mocked(useUpdateUserLocaleSettings).mockReturnValue(mockUpdateUserLocaleSettings);
        vi.mocked(useUserLanguage).mockReturnValue(null);
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

    it('records clearing a saved language as newLanguage null', async () => {
        vi.mocked(useUserLanguage).mockReturnValue('de');
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await result.current(null);
        });

        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.LanguageChangeEvent,
            previousLanguage: 'en',
            newLanguage: null,
            browserLanguage: 'fr',
        });
    });

    it('does not record re-selecting the language that is already saved', async () => {
        vi.mocked(useUserLanguage).mockReturnValue('de');
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await result.current('de');
        });

        expect(analytics.event).not.toHaveBeenCalled();
    });

    it('records explicitly picking the language the browser already gave the user', async () => {
        vi.spyOn(i18next, 'language', 'get').mockReturnValue('fr');
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await result.current('fr');
        });

        expect(analytics.event).toHaveBeenCalledWith(
            expect.objectContaining({ previousLanguage: 'fr', newLanguage: 'fr' }),
        );
    });

    it('records the change with the previous, new and browser-detected language', async () => {
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await result.current('de');
        });

        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.LanguageChangeEvent,
            previousLanguage: 'en',
            newLanguage: 'de',
            browserLanguage: 'fr',
        });
    });

    it('does not record a change when saving the setting fails', async () => {
        mockUpdateUserLocaleSettings.mockRejectedValueOnce(new Error('save failed'));
        const { result } = renderHook(() => useChangeLocale());

        await act(async () => {
            await expect(result.current('de')).rejects.toThrow();
        });

        expect(analytics.event).not.toHaveBeenCalled();
    });
});
